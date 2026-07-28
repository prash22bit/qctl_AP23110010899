# Architecture & Technical Decisions (DECISIONS.md)

This document answers the 5 mandatory design questions for the **QueueCTL** background job queue system.

---

### Question 1: Atomic Job Claiming Across Processes

**Which exact line(s) prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?**

- **Location**: [`src/db.js:154-192`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js#L154-L192)
- **Implementation**:
  ```javascript
  const connection = await db.getConnection();
  try {
    await connection.beginTransaction();

    let rows;
    try {
      [rows] = await connection.query(
        `SELECT * FROM jobs 
         WHERE state = 'pending' OR (state = 'failed' AND next_run_at <= ?) 
         ORDER BY created_at ASC 
         LIMIT 1 
         FOR UPDATE SKIP LOCKED`,
        [nowStr]
      );
    } catch (e) {
      [rows] = await connection.query(
        `SELECT * FROM jobs 
         WHERE state = 'pending' OR (state = 'failed' AND next_run_at <= ?) 
         ORDER BY created_at ASC 
         LIMIT 1 
         FOR UPDATE`,
        [nowStr]
      );
    }

    if (rows.length === 0) {
      await connection.commit();
      return null;
    }

    const job = rows[0];
    await connection.query(
      `UPDATE jobs SET state = 'processing', updated_at = ?, worker_id = ? WHERE id = ?`,
      [nowStr, workerId, job.id]
    );

    await connection.commit();
    return job;
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    connection.release();
  }
  ```

#### Why it is atomic across separate OS processes:
MySQL InnoDB storage engine provides transaction isolation and row-level locking. When multiple worker processes—running as separate OS processes in different terminal sessions—call `claimNextJob` simultaneously:
1. Worker A opens a transaction and executes `SELECT ... FOR UPDATE SKIP LOCKED` (or `FOR UPDATE`).
2. InnoDB locks the candidate job row for exclusive writing by Worker A.
3. Concurrent Worker B executing `FOR UPDATE SKIP LOCKED` automatically skips the row locked by Worker A and selects the next eligible job, or blocks safely until Worker A's transaction commits.
4. Worker A updates the job state to `'processing'`, sets `worker_id`, and commits the transaction.
5. Once committed, Worker B (or future claim calls) sees the updated state `'processing'` and will not claim the same job.

Because row-level write locks are managed inside MySQL InnoDB itself, race conditions are impossible regardless of how many OS worker processes run in parallel.

---

### Question 2: SIGKILL Crash Recovery Walkthrough

**A worker is SIGKILLed halfway through a job. Walk through, step by step, what state the job is in and how it eventually runs again. What is the worst-case delay before recovery?**

#### Step-by-Step Walkthrough:
1. **Worker Claims Job**: Worker A calls `claimNextJob('worker-123')`. Job state transitions from `pending` -> `processing`, and `updated_at` is set to `T0`.
2. **Worker Killed (`SIGKILL`)**: Mid-execution, Worker A receives `SIGKILL` (`kill -9`). Operating systems terminate the process instantly without running any exit hooks or signal handlers.
3. **Stuck Job State**: The job remains in `processing` state in MySQL with `worker_id: 'worker-123'` and `updated_at: T0`.
4. **Crash Recovery Detection**: Active workers (or new worker processes) execute `recoverStaleJobs(30)` at the beginning of their polling loop ([`src/worker-child.js:55`](file:///Users/prashhh/Desktop/flam%20assignment/src/worker-child.js#L55) & [`src/db.js:235-245`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js#L235-L245)).
5. **Stale Job Recovery Query**:
   ```javascript
   const cutoff = new Date(Date.now() - 30 * 1000).toISOString();
   await db.query(
     `UPDATE jobs SET state = 'pending', updated_at = ?, worker_id = NULL WHERE state = 'processing' AND updated_at < ?`,
     [nowStr, cutoff]
   );
   ```
   The query finds the job because its `updated_at` (`T0`) is older than 30 seconds ago.
6. **State Reset**: The job state is updated back to `pending`, and `worker_id` is set back to `NULL`.
7. **Re-execution**: On the next poll cycle, Worker B claims the job and executes it to completion.

#### Worst-Case Delay Before Recovery:
- Stale heartbeat threshold: `30 seconds`
- Worker polling sleep interval: `1 second`
- **Worst-case total recovery delay**: **~31 seconds** (well below the required 60-second limit).

---

### Question 3: DLQ Retry Behavior (`dlq retry <id>`)

**Does `dlq retry` reset attempts? Why is that the right call?**

- **Decision**: Yes, `queuectl dlq retry <id>` resets `attempts` to `0` ([`src/db.js:250-272`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js#L250-L272)).

#### Justification:
When a job enters the Dead Letter Queue (`dead`), it has exhausted all automated retries under its original conditions (e.g. external API outage, bad file path, database connection failure).

Moving a job out of DLQ via manual CLI command (`queuectl dlq retry <id>`) represents an explicit human operator action indicating that the underlying cause of failure has been rectified. Resetting `attempts = 0` provides the job with a fresh retry budget (`max_retries`) and resets initial exponential backoff delays. If `attempts` were retained at max retries (e.g., `3/3`), a single transient glitch after manual intervention would immediately return the job to DLQ without allowing standard retries.

---

### Question 4: Cross-Process Worker Signaling (`worker stop`)

**What designs did you consider and reject for `worker stop` (cross-process signaling), and why?**

#### Designs Considered and Rejected:
1. **Database Stopping Flag (`stopping: true`)**:
   - *Why Rejected*: If a worker is executing a long-running synchronous shell command (e.g., `sleep 30`), it is blocked in `child_process.exec()` and cannot poll MySQL until execution finishes. A database flag cannot interrupt or notify a blocked OS process.
2. **IPC Sockets / Unix Domain Sockets**:
   - *Why Rejected*: Requires managing socket files, connection pools, port conflicts, and socket cleanup on abrupt crashes. Adds architectural overhead without extra benefits over OS signals.

#### Chosen Design: Database PID Registry + Native OS `SIGTERM` Signals
- **Mechanism**:
   1. Workers register their OS Process IDs (`pid`) in MySQL `workers` table upon start ([`src/db.js:296-311`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js#L296-L311)).
   2. When `queuectl worker stop` is executed from another terminal, it reads active worker PIDs from MySQL and issues OS `SIGTERM` (`process.kill(pid, 'SIGTERM')`) ([`src/worker-manager.js:46-70`](file:///Users/prashhh/Desktop/flam%20assignment/src/worker-manager.js#L46-L70)).
   3. Worker processes catch `SIGTERM` ([`src/worker-child.js:17-33`](file:///Users/prashhh/Desktop/flam%20assignment/src/worker-child.js#L17-L33)), set `isShuttingDown = true`, finish their current in-flight job, and exit cleanly.

---

### Question 5: Priority Queue Extensibility Analysis

**If priorities were added tomorrow (high-priority jobs jump the queue), which parts of your design survive unchanged and which break?**

#### Parts That Survive Unchanged:
- **Worker Management & Pool**: [`src/worker-manager.js`](file:///Users/prashhh/Desktop/flam%20assignment/src/worker-manager.js) and process signal handlers survive completely unchanged.
- **Crash Recovery Mechanism**: [`recoverStaleJobs`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js#L235-L245) logic remains identical.
- **DLQ & Configuration Management**: DLQ retry and config key-value store survive unchanged.

#### Parts That Change / Break:
1. **Job Schema**: Must add a `priority` column to `jobs` table (e.g., `INT DEFAULT 5`).
2. **MySQL Compound Index**: Update compound index from `(state, next_run_at, created_at)` to `(priority DESC, state, next_run_at, created_at)`.
3. **Atomic Claiming Sort Order**: In `claimNextJob` ([`src/db.js:163`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js#L163)), change `ORDER BY` clause from `ORDER BY created_at ASC` to `ORDER BY priority DESC, created_at ASC`. High-priority jobs automatically jump the queue without altering worker claim logic.
4. **CLI `enqueue`**: Add optional `--priority <number>` flag to CLI argument parser.
