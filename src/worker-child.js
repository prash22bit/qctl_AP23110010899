const { exec } = require('child_process');
const db = require('./db');

class WorkerChild {
  constructor(workerId) {
    this.workerId = workerId || `worker-${process.pid}`;
    this.isShuttingDown = false;
    this.currentJob = null;
    this.heartbeatInterval = null;
  }

  async start() {
    // Register worker PID in DB
    await db.registerWorker(process.pid, { worker_id: this.workerId });

    // Handle Graceful Shutdown signals
    const stopHandler = async (signal) => {
      if (this.isShuttingDown) return;
      this.isShuttingDown = true;
      // Finish current job before exiting
      if (this.currentJob) {
        // Log to stderr so stdout remains clean for --json commands
        console.error(`[Worker ${process.pid}] Received ${signal}, finishing in-flight job ${this.currentJob.id}...`);
      } else {
        console.error(`[Worker ${process.pid}] Received ${signal}, exiting...`);
        await db.unregisterWorker(process.pid);
        await db.closeDb();
        process.exit(0);
      }
    };

    process.on('SIGTERM', () => stopHandler('SIGTERM'));
    process.on('SIGINT', () => stopHandler('SIGINT'));

    // Periodic heartbeat to worker registry & stale job recovery check
    this.heartbeatInterval = setInterval(async () => {
      try {
        await db.updateWorkerHeartbeat(process.pid);
        if (this.currentJob) {
          await db.updateJobHeartbeat(this.currentJob.id);
        }
      } catch (err) {
        // Ignore heartbeat DB transient errors
      }
    }, 5000);

    // Main worker polling loop
    await this.loop();
  }

  async loop() {
    while (!this.isShuttingDown) {
      try {
        // 1. Run crash recovery for stale processing jobs (>30s)
        await db.recoverStaleJobs(30);

        // 2. Atomically claim next pending or retryable job
        const job = await db.claimNextJob(this.workerId);

        if (job) {
          this.currentJob = job;
          console.error(`[Worker ${process.pid}] Claimed job ${job.id}: "${job.command}"`);
          
          await this.executeJob(job);
          
          this.currentJob = null;
        } else {
          // No job available, sleep briefly before polling again
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      } catch (err) {
        console.error(`[Worker ${process.pid}] Error in polling loop:`, err.message);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    // After breaking loop due to shutdown flag and finishing job
    clearInterval(this.heartbeatInterval);
    await db.unregisterWorker(process.pid);
    await db.closeDb();
    process.exit(0);
  }

  executeJob(job) {
    return new Promise((resolve) => {
      exec(job.command, { timeout: 300000 }, async (error, stdout, stderr) => {
        if (error) {
          const errorMsg = stderr.trim() || error.message || `Exit code ${error.code}`;
          console.error(`[Worker ${process.pid}] Job ${job.id} FAILED: ${errorMsg}`);
          await db.markJobFailed(job.id, errorMsg);
        } else {
          console.error(`[Worker ${process.pid}] Job ${job.id} COMPLETED successfully.`);
          await db.markJobCompleted(job.id);
        }
        resolve();
      });
    });
  }
}

// Standalone execution if spawned directly as a child process
if (require.main === module) {
  const worker = new WorkerChild();
  worker.start().catch(err => {
    console.error('Fatal worker error:', err);
    process.exit(1);
  });
}

module.exports = WorkerChild;
