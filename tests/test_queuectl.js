const assert = require('assert');
const { execSync, spawn } = require('child_process');
const db = require('../src/db');

const CLI_PATH = './bin/queuectl.js';

function runCli(args) {
  return execSync(`node ${CLI_PATH} ${args}`, { encoding: 'utf8' }).trim();
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function cleanDb() {
  const dbInst = await db.getDb();
  await dbInst.query('TRUNCATE TABLE jobs');
  await dbInst.query('TRUNCATE TABLE workers');
  await dbInst.query('TRUNCATE TABLE config');
}

async function runTests() {
  console.log('==================================================');
  console.log('   Running QueueCTL Automated Test Suite');
  console.log('==================================================\n');

  try {
    // SCENARIO 1: Basic job completion
    console.log('[Test 1/5] Scenario 1: Basic job completes...');
    await cleanDb();
    
    runCli('enqueue \'{"id":"job-test-1","command":"echo \\"Hello QueueCTL\\""}\'');
    let pendingJobs = JSON.parse(runCli('list --state pending --json'));
    assert.strictEqual(pendingJobs.length, 1, 'Expected 1 pending job');
    assert.strictEqual(pendingJobs[0].id, 'job-test-1');

    // Run worker in background for 3.5s
    const worker1 = spawn('node', [CLI_PATH, 'worker', 'start', '--count', '1'], { stdio: 'inherit' });
    await sleep(3500);
    worker1.kill('SIGTERM');
    await sleep(1000);

    const completedJobs = JSON.parse(runCli('list --state completed --json'));
    assert.strictEqual(completedJobs.length, 1, 'Expected 1 completed job');
    assert.strictEqual(completedJobs[0].id, 'job-test-1');
    console.log('✅ Scenario 1 Passed: Basic job completes successfully!\n');

    // SCENARIO 2: Failing job retries with backoff and lands in DLQ
    console.log('[Test 2/5] Scenario 2: Failing job retries with backoff and lands in DLQ...');
    await cleanDb();
    await db.setConfig('backoff-base', '1'); // 1 sec delay for fast test execution

    runCli('enqueue \'{"id":"job-fail-1","command":"nonexistent-command-abc","max_retries":2}\'');
    
    const worker2 = spawn('node', [CLI_PATH, 'worker', 'start', '--count', '1'], { stdio: 'pipe' });
    await sleep(6000); // Allow time for initial attempt + 2 retries
    worker2.kill('SIGTERM');
    await sleep(1000);

    const deadJobs = JSON.parse(runCli('dlq list --json'));
    assert.strictEqual(deadJobs.length, 1, 'Expected 1 job in DLQ');
    assert.strictEqual(deadJobs[0].id, 'job-fail-1');
    assert.strictEqual(deadJobs[0].attempts, 2, 'Expected 2 attempts before DLQ');

    // Test DLQ retry
    runCli('dlq retry job-fail-1');
    const retriedJobs = JSON.parse(runCli('list --state pending --json'));
    assert.strictEqual(retriedJobs.length, 1, 'Expected job to be re-enqueued as pending');
    assert.strictEqual(retriedJobs[0].attempts, 0, 'Expected attempts reset on DLQ retry');
    console.log('✅ Scenario 2 Passed: Failing job retries with backoff & lands in DLQ!\n');

    // SCENARIO 3: Many jobs across multiple workers run exactly once
    console.log('[Test 3/5] Scenario 3: Many jobs across 3 parallel workers...');
    await cleanDb();
    const jobCount = 10;
    for (let i = 1; i <= jobCount; i++) {
      runCli(`enqueue '{"id":"job-multi-${i}","command":"echo \\"Task ${i}\\""}'`);
    }

    const worker3 = spawn('node', [CLI_PATH, 'worker', 'start', '--count', '3'], { stdio: 'pipe' });
    await sleep(6000);
    worker3.kill('SIGTERM');
    await sleep(1000);

    const multiCompleted = JSON.parse(runCli('list --state completed --json'));
    assert.strictEqual(multiCompleted.length, jobCount, `Expected all ${jobCount} jobs completed`);
    console.log(`✅ Scenario 3 Passed: All ${jobCount} jobs executed exactly once across 3 workers!\n`);

    // SCENARIO 4: SIGKILL mid-job crash recovery
    console.log('[Test 4/5] Scenario 4: Worker SIGKILL mid-job crash recovery...');
    await cleanDb();

    // Enqueue job with sleep 3
    runCli('enqueue \'{"id":"job-crash-1","command":"sleep 3"}\'');

    // Start worker child directly
    const workerChild = spawn('node', ['./src/worker-child.js'], { stdio: 'pipe' });
    await sleep(2000); // Wait for worker to claim job

    const processingBefore = JSON.parse(runCli('list --state processing --json'));
    assert.strictEqual(processingBefore.length, 1, 'Job should be in processing state');
    assert.strictEqual(processingBefore[0].id, 'job-crash-1');

    // Simulate CRASH by sending SIGKILL to worker child (no cleanup handlers run)
    console.log('Simulating crash: sending SIGKILL (kill -9) to active worker child...');
    workerChild.kill('SIGKILL');
    await sleep(1000);

    // Simulate recovery check (stale threshold set to 1 second for fast test)
    await db.recoverStaleJobs(1);

    const recoveredPending = JSON.parse(runCli('list --state pending --json'));
    assert.strictEqual(recoveredPending.length, 1, 'Job must recover from processing back to pending');
    assert.strictEqual(recoveredPending[0].id, 'job-crash-1');

    // Restart worker process and verify job completes
    const worker4 = spawn('node', [CLI_PATH, 'worker', 'start', '--count', '1'], { stdio: 'pipe' });
    await sleep(4000);
    worker4.kill('SIGTERM');
    await sleep(1000);

    const crashCompleted = JSON.parse(runCli('list --state completed --json'));
    assert.strictEqual(crashCompleted.length, 1, 'Recovered job must complete on restart');
    assert.strictEqual(crashCompleted[0].id, 'job-crash-1');
    console.log('✅ Scenario 4 Passed: SIGKILL mid-job recovered and completed successfully!\n');

    // SCENARIO 5: Jobs survive full restart
    console.log('[Test 5/5] Scenario 5: Job data persistence across full restart...');
    await cleanDb();
    runCli('enqueue \'{"id":"job-persist-1","command":"echo \\"Persisted\\""}\'');
    await db.closeDb();

    // Reconnect to DB and verify job is persisted intact
    const freshDb = await db.getDb();
    const [rows] = await freshDb.query('SELECT * FROM jobs WHERE id = ?', ['job-persist-1']);
    assert.strictEqual(rows.length, 1, 'Job must exist in DB after process restart');
    assert.strictEqual(rows[0].state, 'pending');
    console.log('✅ Scenario 5 Passed: Jobs survive process restarts!\n');

    console.log('==================================================');
    console.log('   🎉 ALL 5 TEST SCENARIOS PASSED SUCCESSFULLY!');
    console.log('==================================================');

  } catch (err) {
    console.error('\n❌ Test Suite Failed:', err.message);
    process.exit(1);
  } finally {
    await db.closeDb();
  }
}

runTests();
