# CrashQueue

A durable job queue in a single SQLite file. Run the demo to watch a worker exit mid-job, a replacement reclaim the job, and the queue reject an acknowledgment from the expired attempt.

**Node.js 24+ · SQLite · Zero npm dependencies**

```bash
npm run demo
npm test
```

```text
CrashQueue / worker recovery

  Producer   Enqueued render-preview
  Worker A   Claimed attempt 1, then exited before acknowledging it
  Worker B   Reopened the database and reclaimed attempt 2
  Queue      Rejected an acknowledgment carrying attempt 1
  Queue      Accepted attempt 2 / final state: done
```

## How recovery works

| Event | What SQLite records |
| --- | --- |
| A worker claims a job | One write transaction picks an eligible job, advances its attempt number, and sets a lease deadline. |
| A worker disappears | The attempt becomes eligible for reclaim when its lease expires. An expired final attempt moves to `dead` on the next claim. |
| An earlier worker returns | An acknowledgment only commits when the job is running, the attempt matches, and the lease remains live. |
| A worker reports a failure | The queue retries after 250 ms, 500 ms, 1 s, and so on up to a 30 s delay, or moves the job to `dead` at its attempt limit. |

Selection and update happen under SQLite's writer lock. Each transaction samples the clock after acquiring that lock, so time spent waiting on a competing writer counts against an existing lease. `UPDATE … RETURNING` hands the winning worker its new attempt number in the same transaction. [SQLite documentation](https://www.sqlite.org/lang_returning.html)

## Use it in a worker

```js
import { Queue } from './queue.mjs';

const queue = new Queue('jobs.sqlite');
try {
  queue.enqueue({ task: 'render-preview' }, { maxAttempts: 3 });
  const job = queue.claim({ leaseMs: 30_000 });
  if (job) {
    console.log(job.payload);
    if (!queue.complete(job)) throw new Error('Lease expired');
  }
} finally {
  queue.close();
}
```

Use `queue.fail(job, error)` to report a failed attempt and `queue.get(id)` to inspect its state. Both acknowledgment methods return `false` when the lease is stale.

## Guarantees and limits

Workers share a database file on a single machine. A task can run more than once if a worker performs an external side effect and exits before acknowledgment. Use the job ID as the downstream idempotency key where the destination supports it. Choose a lease long enough for the handler because leases have a fixed deadline. Completed and dead jobs stay in the database for inspection.

The test suite covers scheduling, stale acknowledgments, retry timing, final lease expiry, and eight independent processes competing for the same job.

*Built with AI assistance as a small systems project.*
