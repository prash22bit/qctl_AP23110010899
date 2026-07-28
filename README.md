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

[Link to CLI Demo Video](https://github.com/your-username/queuectl)
