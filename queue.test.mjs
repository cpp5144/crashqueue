import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { promisify } from 'node:util';
import { Queue } from './queue.mjs';

const exec = promisify(execFile);

function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'crashqueue-test-'));
  const path = join(dir, 'jobs.sqlite');
  const time = { now: 1_000 };
  const connections = [];
  const open = () => {
    const queue = new Queue(path, { clock: () => time.now });
    connections.push(queue);
    return queue;
  };
  t.after(() => {
    for (const queue of connections) queue.close();
    rmSync(dir, { recursive: true, force: true });
  });
  return { path, time, open };
}

test('a delayed job becomes claimable at its scheduled time', (t) => {
  const { open, time } = fixture(t);
  const queue = open();
  const id = queue.enqueue({ task: 'index' }, { delayMs: 50 });

  time.now = 1_049;
  assert.equal(queue.claim(), null);
  time.now = 1_050;
  const claim = queue.claim();
  assert.equal(claim.id, id);
  assert.deepEqual(claim.payload, { task: 'index' });
});

test('a second connection reclaims an expired lease and rejects the old attempt', (t) => {
  const { open, time } = fixture(t);
  const firstWorker = open();
  const secondWorker = open();
  const id = firstWorker.enqueue('render');
  const first = firstWorker.claim({ leaseMs: 10 });

  time.now = 1_009;
  assert.equal(secondWorker.claim(), null);
  time.now = 1_010;
  assert.equal(firstWorker.complete(first), false);
  const second = secondWorker.claim({ leaseMs: 10 });

  assert.equal(second.id, id);
  assert.equal(second.attempt, 2);
  assert.equal(firstWorker.fail(first, 'stale'), false);
  assert.equal(firstWorker.complete(first), false);
  assert.equal(secondWorker.complete(second), true);
  assert.equal(firstWorker.get(id).state, 'done');
});

test('explicit failures back off before retrying and stop at the attempt limit', (t) => {
  const { open, time } = fixture(t);
  const queue = open();
  const id = queue.enqueue('fetch', { maxAttempts: 3 });

  assert.equal(queue.fail(queue.claim(), 'first failure'), true);
  time.now = 1_249;
  assert.equal(queue.claim(), null);
  time.now = 1_250;
  assert.equal(queue.fail(queue.claim(), 'second failure'), true);
  time.now = 1_749;
  assert.equal(queue.claim(), null);
  time.now = 1_750;
  assert.equal(queue.fail(queue.claim(), 'third failure'), true);
  assert.equal(queue.get(id).state, 'dead');
  assert.equal(queue.get(id).lastError, 'third failure');
  assert.equal(queue.claim(), null);
});

test('an expired final attempt becomes dead on the next claim', (t) => {
  const { open, time } = fixture(t);
  const queue = open();
  const id = queue.enqueue('export', { maxAttempts: 1 });
  queue.claim({ leaseMs: 10 });

  time.now = 1_010;
  assert.equal(queue.claim(), null);
  assert.equal(queue.get(id).state, 'dead');
  assert.equal(queue.get(id).lastError, 'Lease expired on final attempt');
});

test('eight independent processes can claim only one live lease for a job', async (t) => {
  const { open, path } = fixture(t);
  const queue = open();
  const id = queue.enqueue({ task: 'compete' });
  const script = `
    import { Queue } from ${JSON.stringify(new URL('./queue.mjs', import.meta.url).href)};
    const queue = new Queue(process.argv[1], { clock: () => 1_000 });
    console.log(JSON.stringify(queue.claim()));
    queue.close();
  `;
  const outcomes = await Promise.allSettled(Array.from({ length: 8 }, async () => {
    const { stdout } = await exec(process.execPath, ['--input-type=module', '-e', script, path]);
    return JSON.parse(stdout);
  }));
  const errors = outcomes.filter((outcome) => outcome.status === 'rejected');
  assert.deepEqual(errors.map((outcome) => outcome.reason.message), []);
  const claims = outcomes.map((outcome) => outcome.value).filter(Boolean);
  assert.deepEqual(claims.map(({ id, attempt }) => [id, attempt]), [[id, 1]]);
});
