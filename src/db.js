const mysql = require('mysql2/promise');

const MYSQL_HOST = process.env.QUEUECTL_MYSQL_HOST || '127.0.0.1';
const MYSQL_PORT = parseInt(process.env.QUEUECTL_MYSQL_PORT || '3306', 10);
const MYSQL_USER = process.env.QUEUECTL_MYSQL_USER || 'root';
const MYSQL_PASSWORD = process.env.QUEUECTL_MYSQL_PASSWORD !== undefined ? process.env.QUEUECTL_MYSQL_PASSWORD : '';
const MYSQL_DATABASE = process.env.QUEUECTL_MYSQL_DATABASE || 'queuectl';

let poolInstance = null;

/**
 * Ensure database exists and return connection pool instance
 */
async function getDb() {
  if (poolInstance) {
    return poolInstance;
  }

  // 1. Connect without database selected to ensure DB exists
  try {
    const tempConn = await mysql.createConnection({
      host: MYSQL_HOST,
      port: MYSQL_PORT,
      user: MYSQL_USER,
      password: MYSQL_PASSWORD,
      connectTimeout: 5000
    });
    await tempConn.query(`CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\``);
    await tempConn.end();
  } catch (err) {
    // If DB creation fails (e.g. user lacks CREATE DB privileges), attempt connecting directly
  }

  // 2. Create connection pool
  poolInstance = mysql.createPool({
    host: MYSQL_HOST,
    port: MYSQL_PORT,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 20,
    queueLimit: 0,
    connectTimeout: 5000
  });

  // Initialize tables and indexes
  await initTables(poolInstance);
  return poolInstance;
}

/**
 * Close connection pool
 */
async function closeDb() {
  if (poolInstance) {
    await poolInstance.end();
    poolInstance = null;
  }
}

/**
 * Create tables and indexes if they do not exist
 */
async function initTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS config (
      id VARCHAR(255) PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at VARCHAR(64) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id VARCHAR(255) PRIMARY KEY,
      command TEXT NOT NULL,
      state VARCHAR(50) NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      max_retries INT NOT NULL DEFAULT 3,
      created_at VARCHAR(64) NOT NULL,
      updated_at VARCHAR(64) NOT NULL,
      next_run_at VARCHAR(64) NOT NULL,
      worker_id VARCHAR(255) DEFAULT NULL,
      error TEXT DEFAULT NULL,
      INDEX idx_state_next_created (state, next_run_at, created_at),
      INDEX idx_updated_at (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS workers (
      pid INT PRIMARY KEY,
      started_at VARCHAR(64) NOT NULL,
      last_heartbeat VARCHAR(64) NOT NULL,
      status VARCHAR(50) NOT NULL DEFAULT 'running',
      worker_id VARCHAR(255) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);
}

/**
 * Get configuration setting from DB
 */
async function getConfig(key, defaultValue = null) {
  const db = await getDb();
  const [rows] = await db.query('SELECT value FROM config WHERE id = ?', [key]);
  return rows.length > 0 ? rows[0].value : defaultValue;
}

/**
 * Set configuration setting in DB
 */
async function setConfig(key, value) {
  const db = await getDb();
  const now = new Date().toISOString();
  await db.query(
    `INSERT INTO config (id, value, updated_at) 
     VALUES (?, ?, ?) 
     ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = VALUES(updated_at)`,
    [key, String(value), now]
  );
}

/**
 * Enqueue a new job or update existing
 */
async function enqueueJob(jobSpec) {
  const db = await getDb();
  const now = new Date().toISOString();
  const defaultMaxRetries = parseInt(await getConfig('max-retries', 3), 10);

  const job = {
    id: jobSpec.id,
    command: jobSpec.command,
    state: 'pending',
    attempts: jobSpec.attempts !== undefined ? jobSpec.attempts : 0,
    max_retries: jobSpec.max_retries !== undefined ? jobSpec.max_retries : defaultMaxRetries,
    created_at: jobSpec.created_at || now,
    updated_at: now,
    next_run_at: now,
    worker_id: null,
    error: null
  };

  await db.query(
    `INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at, worker_id, error)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE 
       command = VALUES(command),
       state = VALUES(state),
       attempts = VALUES(attempts),
       max_retries = VALUES(max_retries),
       created_at = VALUES(created_at),
       updated_at = VALUES(updated_at),
       next_run_at = VALUES(next_run_at),
       worker_id = VALUES(worker_id),
       error = VALUES(error)`,
    [
      job.id,
      job.command,
      job.state,
      job.attempts,
      job.max_retries,
      job.created_at,
      job.updated_at,
      job.next_run_at,
      job.worker_id,
      job.error
    ]
  );

  return job;
}

/**
 * Atomic Job Claiming via MySQL Transaction with SELECT ... FOR UPDATE.
 * Operates atomically across separate OS processes using InnoDB row-level write locks.
 */
async function claimNextJob(workerId) {
  const db = await getDb();
  const nowStr = new Date().toISOString();

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
      // Fallback for MySQL versions without SKIP LOCKED syntax
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

    job.state = 'processing';
    job.updated_at = nowStr;
    job.worker_id = workerId;
    return job;
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    connection.release();
  }
}

/**
 * Update Heartbeat for an in-flight job to prevent crash recovery from taking it
 */
async function updateJobHeartbeat(jobId) {
  const db = await getDb();
  await db.query(
    `UPDATE jobs SET updated_at = ? WHERE id = ? AND state = 'processing'`,
    [new Date().toISOString(), jobId]
  );
}

/**
 * Mark job completed
 */
async function markJobCompleted(jobId) {
  const db = await getDb();
  const nowStr = new Date().toISOString();
  await db.query(
    `UPDATE jobs SET state = 'completed', updated_at = ?, worker_id = NULL, error = NULL WHERE id = ?`,
    [nowStr, jobId]
  );
}

/**
 * Mark job failed, calculate exponential backoff delay or move to DLQ (dead)
 */
async function markJobFailed(jobId, errorMsg) {
  const db = await getDb();
  const [rows] = await db.query('SELECT * FROM jobs WHERE id = ?', [jobId]);
  if (rows.length === 0) return;
  const job = rows[0];

  const newAttempts = job.attempts + 1;
  const now = new Date();
  const nowStr = now.toISOString();

  if (newAttempts >= job.max_retries) {
    // Retries exhausted -> Dead Letter Queue (DLQ)
    await db.query(
      `UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ?, worker_id = NULL, error = ? WHERE id = ?`,
      [newAttempts, nowStr, errorMsg, jobId]
    );
  } else {
    // Retryable failure -> Exponential backoff delay
    const backoffBase = parseFloat(await getConfig('backoff-base', 2));
    const delaySeconds = Math.pow(backoffBase, newAttempts);
    const nextRun = new Date(now.getTime() + delaySeconds * 1000).toISOString();

    await db.query(
      `UPDATE jobs SET state = 'failed', attempts = ?, next_run_at = ?, updated_at = ?, worker_id = NULL, error = ? WHERE id = ?`,
      [newAttempts, nextRun, nowStr, errorMsg, jobId]
    );
  }
}

/**
 * Crash Recovery: Reset jobs stuck in 'processing' whose updated_at is older than staleSeconds back to 'pending'.
 */
async function recoverStaleJobs(staleSeconds = 30) {
  const db = await getDb();
  const cutoff = new Date(Date.now() - staleSeconds * 1000).toISOString();
  const nowStr = new Date().toISOString();

  const [result] = await db.query(
    `UPDATE jobs SET state = 'pending', updated_at = ?, worker_id = NULL WHERE state = 'processing' AND updated_at < ?`,
    [nowStr, cutoff]
  );
  return result.affectedRows;
}

/**
 * Re-enqueue a dead job from DLQ.
 * Resets state to 'pending' so it can run again.
 */
async function retryDlqJob(jobId, resetAttempts = true) {
  const db = await getDb();
  const [rows] = await db.query(`SELECT * FROM jobs WHERE id = ? AND state = 'dead'`, [jobId]);
  if (rows.length === 0) {
    throw new Error(`Job '${jobId}' not found in DLQ (state='dead')`);
  }

  const nowStr = new Date().toISOString();
  if (resetAttempts) {
    await db.query(
      `UPDATE jobs SET state = 'pending', attempts = 0, updated_at = ?, next_run_at = ?, worker_id = NULL, error = NULL WHERE id = ?`,
      [nowStr, nowStr, jobId]
    );
  } else {
    await db.query(
      `UPDATE jobs SET state = 'pending', updated_at = ?, next_run_at = ?, worker_id = NULL, error = NULL WHERE id = ?`,
      [nowStr, nowStr, jobId]
    );
  }
}

/**
 * Get jobs filtered by state
 */
async function getJobsByState(state) {
  const db = await getDb();
  let query = 'SELECT * FROM jobs ORDER BY created_at ASC';
  let params = [];
  if (state) {
    query = 'SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC';
    params = [state];
  }
  const [rows] = await db.query(query, params);
  return rows.map(doc => ({
    id: doc.id,
    command: doc.command,
    state: doc.state,
    attempts: doc.attempts,
    max_retries: doc.max_retries,
    created_at: doc.created_at,
    updated_at: doc.updated_at
  }));
}

/**
 * Register active worker process
 */
async function registerWorker(pid, metadata = {}) {
  const db = await getDb();
  const nowStr = new Date().toISOString();
  const workerId = metadata.worker_id || `worker-${pid}`;

  await db.query(
    `INSERT INTO workers (pid, started_at, last_heartbeat, status, worker_id)
     VALUES (?, ?, ?, 'running', ?)
     ON DUPLICATE KEY UPDATE 
       started_at = VALUES(started_at),
       last_heartbeat = VALUES(last_heartbeat),
       status = VALUES(status),
       worker_id = VALUES(worker_id)`,
    [pid, nowStr, nowStr, workerId]
  );
}

/**
 * Update worker heartbeat & fetch active workers
 */
async function updateWorkerHeartbeat(pid) {
  const db = await getDb();
  await db.query(
    `UPDATE workers SET last_heartbeat = ? WHERE pid = ?`,
    [new Date().toISOString(), pid]
  );
}

/**
 * Unregister worker process
 */
async function unregisterWorker(pid) {
  const db = await getDb();
  await db.query('DELETE FROM workers WHERE pid = ?', [pid]);
}

/**
 * List active workers (whose heartbeat was within last 15s and status is running)
 */
async function getActiveWorkers() {
  const db = await getDb();
  const cutoff = new Date(Date.now() - 15000).toISOString();
  const [rows] = await db.query(
    `SELECT * FROM workers WHERE status = 'running' AND last_heartbeat >= ?`,
    [cutoff]
  );
  return rows;
}

/**
 * Get system status summary
 */
async function getStatusSummary() {
  const db = await getDb();
  const [rows] = await db.query('SELECT state, COUNT(*) as count FROM jobs GROUP BY state');
  const summary = {
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0,
    dead: 0
  };
  rows.forEach(item => {
    if (summary.hasOwnProperty(item.state)) {
      summary[item.state] = Number(item.count);
    }
  });

  const activeWorkers = await getActiveWorkers();
  summary.active_workers = activeWorkers.length;
  summary.worker_pids = activeWorkers.map(w => w.pid);
  return summary;
}

module.exports = {
  getDb,
  closeDb,
  getConfig,
  setConfig,
  enqueueJob,
  claimNextJob,
  updateJobHeartbeat,
  markJobCompleted,
  markJobFailed,
  recoverStaleJobs,
  retryDlqJob,
  getJobsByState,
  registerWorker,
  updateWorkerHeartbeat,
  unregisterWorker,
  getActiveWorkers,
  getStatusSummary
};
