# QueueCTL — Background Job Queue System

[![Node.js](https://img.shields.io/badge/Node.js-v18%2B-brightgreen)](https://nodejs.org/)
[![MySQL](https://img.shields.io/badge/MySQL-v8.0%2B-blue)](https://www.mysql.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**QueueCTL** is a production-grade, CLI-based background job queue system built in Node.js with MySQL persistence, parallel worker management, exponential backoff retries, Dead Letter Queue (DLQ), and robust sub-60-second SIGKILL crash recovery.

---

## 🚀 Features

- **CLI-First Contract**: Strict adherence to command interface contract (e.g. `queuectl list --state <state> --json`).
- **Atomic Job Claiming**: Cross-process atomic job reservation via MySQL transactions (`SELECT ... FOR UPDATE`).
- **Parallel Workers**: Multi-process worker pool running in foreground or background across separate terminal sessions.
- **Automatic Exponential Backoff**: Retries failed jobs with configurable base (`delay = base ^ attempts` seconds).
- **Dead Letter Queue (DLQ)**: Permanently failed jobs move to DLQ and can be manually inspected and re-enqueued.
- **SIGKILL Crash Recovery**: Automatic detection and recovery of stuck `processing` jobs (<31s worst-case recovery).
- **Graceful Shutdown**: Workers process in-flight jobs to completion before terminating on `SIGINT` / `SIGTERM`.

---

## 🏗️ Architecture

### System Overview

QueueCTL follows a **multi-process, shared-database** architecture. A single MySQL database acts as the central coordination layer, allowing any number of independent worker processes (across separate terminal sessions or machines) to safely claim and execute jobs without race conditions.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLI Interface (bin/queuectl.js)              │
│          enqueue │ worker start/stop │ list │ status │ dlq │ config  │
└────────┬─────────────────┬──────────────────────┬───────────────────┘
         │                 │                      │
         ▼                 ▼                      ▼
  ┌─────────────┐  ┌───────────────────┐  ┌────────────────┐
  │   src/cli.js │  │ src/worker-       │  │   src/db.js    │
  │  (Commander) │  │  manager.js       │  │ (MySQL Pool +  │
  └──────┬───────┘  │ (Process Pool)    │  │  Atomic Ops)   │
         │          └────────┬──────────┘  └───────┬────────┘
         │                   │                     │
         │          ┌────────┴──────────┐           │
         │          │  Worker Processes  │           │
         │          │ (worker-child.js) │           │
         │          │  x N processes    │           │
         │          └────────┬──────────┘           │
         │                   │                     │
         └───────────────────┴─────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   MySQL 8.0+    │
                    │ ┌─────────────┐ │
                    │ │  jobs table │ │
                    │ │  (InnoDB)   │ │
                    │ ├─────────────┤ │
                    │ │workers table│ │
                    │ │ (PID store) │ │
                    │ ├─────────────┤ │
                    │ │config table │ │
                    │ └─────────────┘ │
                    └─────────────────┘
```

### Job Lifecycle & State Machine

```
                       ┌──────────┐
                       │  enqueue │
                       └────┬─────┘
                            │
                            ▼
                       ┌─────────┐
               ┌──────▶│ pending │◀─────────────────────┐
               │       └────┬────┘                      │
               │            │ claimNextJob()             │
               │            │ (SELECT FOR UPDATE)        │ dlq retry <id>
               │            ▼                            │ (attempts reset)
               │       ┌────────────┐                   │
               │       │ processing │                   │
               │       └─────┬──────┘                   │
               │             │                          │
               │      ┌──────┴──────┐                  │
               │      │             │                  │
               │      ▼             ▼                  │
               │  ┌──────────┐  ┌────────┐            │
               │  │completed │  │ failed │            │
               │  └──────────┘  └───┬────┘            │
               │                    │                  │
               │       attempts < max_retries          │
               └────────────────────┘ (exponential     │
                                        backoff)       │
                                    │                  │
                         attempts == max_retries       │
                                    │                  │
                                    ▼                  │
                               ┌──────┐                │
                               │ dead │────────────────┘
                               │(DLQ) │
                               └──────┘
```

### Core Components

| Component | File | Responsibility |
|-----------|------|----------------|
| **CLI Entrypoint** | [`bin/queuectl.js`](file:///Users/prashhh/Desktop/flam%20assignment/bin/queuectl.js) | Executable entry; delegates to CLI parser |
| **CLI Commands** | [`src/cli.js`](file:///Users/prashhh/Desktop/flam%20assignment/src/cli.js) | Commander.js command definitions: `enqueue`, `worker`, `list`, `status`, `dlq`, `config` |
| **Database Layer** | [`src/db.js`](file:///Users/prashhh/Desktop/flam%20assignment/src/db.js) | MySQL connection pool, table initialization, atomic job claiming (`SELECT ... FOR UPDATE`), crash recovery |
| **Worker Child** | [`src/worker-child.js`](file:///Users/prashhh/Desktop/flam%20assignment/src/worker-child.js) | Single worker execution loop: polls jobs, executes shell commands, handles retries, registers PID, listens for `SIGTERM` |
| **Worker Manager** | [`src/worker-manager.js`](file:///Users/prashhh/Desktop/flam%20assignment/src/worker-manager.js) | Spawns/manages N worker child processes, forwards `SIGTERM`/`SIGINT`, issues `worker stop` via PID lookup |

### Key Design Decisions

#### 1. Atomic Job Claiming — `SELECT ... FOR UPDATE SKIP LOCKED`
Multiple workers race to claim the next job. InnoDB row-level locking inside a transaction ensures only one worker wins per job row, completely eliminating duplicate execution across all OS processes.

#### 2. SIGKILL Crash Recovery (≤31s)
Each worker runs `recoverStaleJobs(30)` at the top of every poll cycle. Any job stuck in `processing` for >30 seconds is reset to `pending`, ensuring crashed workers don't strand jobs. Worst-case recovery: **31 seconds** (30s threshold + 1s poll interval).

#### 3. Exponential Backoff Formula
```
next_run_at = NOW() + (backoff_base ^ attempts) seconds
```
Default `backoff_base = 2`, so delays are 2s, 4s, 8s, 16s… between retries.

#### 4. Cross-Process `worker stop` — DB PID Registry + OS Signals
Workers write their OS PIDs to the `workers` MySQL table on startup. `worker stop` reads those PIDs and sends `SIGTERM`, which workers catch to finish in-flight jobs before exiting cleanly. This avoids polling overhead and socket complexity.

#### 5. MySQL as Single Source of Truth
All state (job status, worker PIDs, configuration) lives in MySQL. This means:
- Workers are stateless and horizontally scalable
- No separate coordination service (Redis, etcd) needed
- Crash recovery is inherently durable (no in-memory state lost)

---

## 📁 Repository Structure

```
├── bin/
│   └── queuectl.js          # CLI executable entrypoint
├── src/
│   ├── db.js                # MySQL connection, tables, atomic transaction queries & recovery
│   ├── worker-child.js      # Worker execution loop & signal handlers
│   ├── worker-manager.js    # Multi-process worker pool manager & signal forwarding
│   └── cli.js               # CLI commands definition (Commander.js)
├── tests/
│   └── test_queuectl.js     # Automated test suite covering scenarios 1-5
├── DECISIONS.md             # Mandatory architecture design answers & trade-offs
├── README.md                # System documentation & usage guide
└── package.json             # Project metadata & dependencies
```

---

## 🛠️ Setup & Installation

### Prerequisites
- **Node.js**: v18.0.0 or higher (`node -v`)
- **MySQL**: Local or remote MySQL instance (`mysql --version`)
  - Supported environment variables:
    - `QUEUECTL_MYSQL_HOST` (default: `127.0.0.1`)
    - `QUEUECTL_MYSQL_PORT` (default: `3306`)
    - `QUEUECTL_MYSQL_USER` (default: `root`)
    - `QUEUECTL_MYSQL_PASSWORD` (default: `""`)
    - `QUEUECTL_MYSQL_DATABASE` (default: `queuectl`)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/queuectl.git
   cd queuectl
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. (Optional) Symlink CLI globally:
   ```bash
   npm link
   ```

---

## 💻 CLI Commands Usage

### 1. Enqueue Jobs
Add background jobs to the queue via JSON string or CLI flags:
```bash
# Via JSON string (Assignment standard)
./bin/queuectl.js enqueue '{"id":"job1","command":"echo Hello World"}'

# With custom retry count
./bin/queuectl.js enqueue '{"id":"job2","command":"sleep 2","max_retries":5}'
```

### 2. Worker Management
Start parallel workers in foreground:
```bash
# Start 3 worker processes in the foreground
./bin/queuectl.js worker start --count 3
```

Gracefully stop all workers from another terminal:
```bash
./bin/queuectl.js worker stop
```

### 3. Check System Status
View job summary counts and active worker process PIDs:
```bash
./bin/queuectl.js status
```

### 4. List Jobs
List jobs by state:
```bash
./bin/queuectl.js list --state pending
```
Output raw JSON array for automated tools:
```bash
./bin/queuectl.js list --state completed --json
```

### 5. Dead Letter Queue (DLQ)
View dead jobs:
```bash
./bin/queuectl.js dlq list --json
```
Re-enqueue a dead job to `pending` state:
```bash
./bin/queuectl.js dlq retry job1
```

### 6. Configuration Management
Configure global settings:
```bash
./bin/queuectl.js config set max-retries 3
./bin/queuectl.js config set backoff-base 2
./bin/queuectl.js config show
```

---

## 🧪 Automated Testing

Run the test suite verifying Scenarios 1–5 (Basic completion, exponential backoff retries & DLQ, multi-worker concurrency, SIGKILL crash recovery, and restart survival):

```bash
npm test
```

---

## 📄 Decisions & Architecture Questions

For detailed answers to the 5 mandatory design questions (atomic locking lines, SIGKILL step-by-step walkthrough, DLQ retry design justification, cross-process signaling, and priority queue evolution), see [DECISIONS.md](file:///Users/prashhh/Desktop/flam%20assignment/DECISIONS.md).

---

## 🎥 CLI Demo Recording

[Link to CLI Demo Video](https://drive.google.com/drive/folders/1SGM4jTSIRQyN5korAs8dbHlFdfVt0cqc?usp=drive_link)
