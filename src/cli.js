const { Command } = require('commander');
const db = require('./db');
const { startWorkers, stopWorkers } = require('./worker-manager');

function setupCLI() {
  const program = new Command();

  program
    .name('queuectl')
    .description('CLI background job queue system with retries, DLQ, and crash recovery')
    .version('1.0.0');

  // 1. ENQUEUE
  program
    .command('enqueue')
    .description('Add a new job to the queue')
    .argument('[jobJson]', 'Job specification as JSON string')
    .option('--id <id>', 'Job ID')
    .option('--command <command>', 'Shell command to execute')
    .option('--max-retries <count>', 'Maximum retry attempts', v => parseInt(v, 10))
    .action(async (jobJson, options) => {
      try {
        let jobSpec = {};
        if (jobJson) {
          try {
            jobSpec = JSON.parse(jobJson);
          } catch (e) {
            console.error('Error: Invalid JSON string provided to enqueue.');
            process.exit(1);
          }
        }
        if (options.id) jobSpec.id = options.id;
        if (options.command) jobSpec.command = options.command;
        if (options.maxRetries !== undefined) jobSpec.max_retries = options.maxRetries;

        if (!jobSpec.id) {
          jobSpec.id = `job-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        }
        if (!jobSpec.command) {
          console.error('Error: Job command is required.');
          process.exit(1);
        }

        const job = await db.enqueueJob(jobSpec);
        console.log(`Job '${job.id}' enqueued successfully.`);
      } catch (err) {
        console.error('Failed to enqueue job:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  // 2. WORKER
  const workerCmd = program.command('worker').description('Worker process management');

  workerCmd
    .command('start')
    .description('Start workers in foreground (blocks until stopped)')
    .option('--count <count>', 'Number of worker processes', v => parseInt(v, 10), 1)
    .action(async (options) => {
      try {
        await startWorkers(options.count);
      } catch (err) {
        console.error('Worker error:', err.message);
        process.exit(1);
      }
    });

  workerCmd
    .command('stop')
    .description('Gracefully stop all running workers from another terminal')
    .action(async () => {
      try {
        await stopWorkers();
      } catch (err) {
        console.error('Failed to stop workers:', err.message);
        process.exit(1);
      }
    });

  // 3. STATUS
  program
    .command('status')
    .description('Summary of all job states & active workers')
    .action(async () => {
      try {
        const summary = await db.getStatusSummary();
        console.log('--- QueueCTL Status ---');
        console.log(`Pending:    ${summary.pending}`);
        console.log(`Processing: ${summary.processing}`);
        console.log(`Completed:  ${summary.completed}`);
        console.log(`Failed:     ${summary.failed}`);
        console.log(`Dead (DLQ): ${summary.dead}`);
        console.log(`Active Workers: ${summary.active_workers} (PIDs: ${summary.worker_pids.join(', ') || 'none'})`);
      } catch (err) {
        console.error('Failed to fetch status:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  // 4. LIST
  program
    .command('list')
    .description('List jobs by state')
    .option('--state <state>', 'Job state filter (pending, processing, completed, failed, dead)')
    .option('--json', 'Print JSON array output')
    .action(async (options) => {
      try {
        const jobs = await db.getJobsByState(options.state);
        if (options.json) {
          // MUST print raw JSON array to stdout and nothing else!
          process.stdout.write(JSON.stringify(jobs, null, 2) + '\n');
        } else {
          console.log(`--- Jobs (${options.state || 'all'}) ---`);
          if (jobs.length === 0) {
            console.log('No jobs found.');
          } else {
            jobs.forEach(j => {
              console.log(`[${j.state.toUpperCase()}] ID: ${j.id} | Cmd: "${j.command}" | Attempts: ${j.attempts}/${j.max_retries}`);
            });
          }
        }
      } catch (err) {
        console.error('Failed to list jobs:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  // 5. DLQ
  const dlqCmd = program.command('dlq').description('Dead Letter Queue management');

  dlqCmd
    .command('list')
    .description('View DLQ (permanently failed dead jobs)')
    .option('--json', 'Print JSON array output')
    .action(async (options) => {
      try {
        const deadJobs = await db.getJobsByState('dead');
        if (options.json) {
          process.stdout.write(JSON.stringify(deadJobs, null, 2) + '\n');
        } else {
          console.log('--- Dead Letter Queue (DLQ) ---');
          if (deadJobs.length === 0) {
            console.log('DLQ is empty.');
          } else {
            deadJobs.forEach(j => {
              console.log(`[DEAD] ID: ${j.id} | Cmd: "${j.command}" | Attempts: ${j.attempts}/${j.max_retries}`);
            });
          }
        }
      } catch (err) {
        console.error('Failed to list DLQ:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  dlqCmd
    .command('retry')
    .description('Re-enqueue a dead job from DLQ')
    .argument('<jobId>', 'ID of job to retry')
    .action(async (jobId) => {
      try {
        await db.retryDlqJob(jobId, true);
        console.log(`Job '${jobId}' re-enqueued from DLQ to pending state.`);
      } catch (err) {
        console.error('Failed to retry DLQ job:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  // 6. CONFIG
  const configCmd = program.command('config').description('Manage configuration');

  configCmd
    .command('set')
    .description('Set configuration option (e.g. max-retries, backoff-base)')
    .argument('<key>', 'Config key')
    .argument('<value>', 'Config value')
    .action(async (key, value) => {
      try {
        await db.setConfig(key, value);
        console.log(`Config '${key}' set to '${value}'.`);
      } catch (err) {
        console.error('Failed to set config:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  configCmd
    .command('show')
    .description('Show current configuration')
    .action(async () => {
      try {
        const maxRetries = await db.getConfig('max-retries', 3);
        const backoffBase = await db.getConfig('backoff-base', 2);
        console.log('--- QueueCTL Config ---');
        console.log(`max-retries:  ${maxRetries}`);
        console.log(`backoff-base: ${backoffBase}`);
      } catch (err) {
        console.error('Failed to fetch config:', err.message);
        process.exit(1);
      } finally {
        await db.closeDb();
      }
    });

  return program;
}

module.exports = { setupCLI };
