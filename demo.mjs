import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Queue } from './queue.mjs';

if (process.argv[2] === '--crash-worker') {
  const worker = new Queue(process.argv[3]);
  const claim = worker.claim({ leaseMs: 1_000 });
  assert.ok(claim);
  writeSync(1, JSON.stringify(claim));
  process.exit(42);
}

const dir = mkdtempSync(join(tmpdir(), 'crashqueue-demo-'));
const path = join(dir, 'demo.sqlite');
let queue;
try {
  queue = new Queue(path);
  const id = queue.enqueue({ task: 'render-preview' });
  console.log('CrashQueue / worker recovery\n');
  console.log('  Producer   Enqueued render-preview');

  const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--crash-worker', path], {
    encoding: 'utf8',
    timeout: 10_000,
  });
  if (child.error) throw child.error;
  assert.equal(child.status, 42, child.stderr);
  const first = JSON.parse(child.stdout);
  assert.equal(first.id, id);
  assert.equal(first.attempt, 1);
  console.log('  Worker A   Claimed attempt 1, then exited before acknowledging it');

  queue.close();
  queue = undefined;
  await setTimeout(Math.max(0, first.leaseUntil - Date.now()) + 20);

  queue = new Queue(path);
  const second = queue.claim({ leaseMs: 30_000 });
  assert.equal(second.id, id);
  assert.equal(second.attempt, 2);
  console.log('  Worker B   Reopened the database and reclaimed attempt 2');

  assert.equal(queue.complete(first), false);
  console.log('  Queue      Rejected an acknowledgment carrying attempt 1');
  assert.equal(queue.complete(second), true);
  assert.equal(queue.get(id).state, 'done');
  console.log('  Queue      Accepted attempt 2 / final state: done');
} finally {
  queue?.close();
  rmSync(dir, { recursive: true, force: true });
}
