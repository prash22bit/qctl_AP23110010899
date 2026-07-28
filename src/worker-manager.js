const { fork } = require('child_process');
const path = require('path');
const db = require('./db');

async function startWorkers(count = 1) {
  console.error(`Starting ${count} worker(s) in foreground... (Press Ctrl+C or run 'queuectl worker stop' to stop)`);

  const workerProcesses = [];
  let isStopping = false;

  const childScript = path.join(__dirname, 'worker-child.js');

  for (let i = 0; i < count; i++) {
    const child = fork(childScript, [], {
      env: process.env,
      stdio: ['inherit', 'inherit', 'inherit', 'ipc']
    });
    workerProcesses.push(child);
  }

  const handleStopSignal = async (signal) => {
    if (isStopping) return;
    isStopping = true;
    console.error(`Manager received ${signal}, initiating graceful shutdown for ${workerProcesses.length} workers...`);

    const exitPromises = workerProcesses.map(child => {
      return new Promise(resolve => {
        child.on('exit', () => resolve());
        child.kill('SIGTERM');
      });
    });

    await Promise.all(exitPromises);
    console.error('All workers stopped gracefully.');
    await db.closeDb();
    process.exit(0);
  };

  process.on('SIGINT', () => handleStopSignal('SIGINT'));
  process.on('SIGTERM', () => handleStopSignal('SIGTERM'));

  // Keep manager process alive while children run
  await new Promise(() => {});
}

async function stopWorkers() {
  const dbInst = await db.getDb();
  const activeWorkers = await db.getActiveWorkers();

  if (activeWorkers.length === 0) {
    console.log('No active workers found.');
    await db.closeDb();
    return;
  }

  console.log(`Sending SIGTERM to ${activeWorkers.length} active worker process(es)...`);
  for (const worker of activeWorkers) {
    try {
      process.kill(worker.pid, 'SIGTERM');
      console.log(`Sent SIGTERM to worker PID ${worker.pid}`);
    } catch (err) {
      if (err.code === 'ESRCH') {
        console.log(`Worker PID ${worker.pid} no longer running. Unregistering...`);
        await db.unregisterWorker(worker.pid);
      } else {
        console.error(`Failed to send SIGTERM to PID ${worker.pid}:`, err.message);
      }
    }
  }
  await db.closeDb();
}

module.exports = {
  startWorkers,
  stopWorkers
};
