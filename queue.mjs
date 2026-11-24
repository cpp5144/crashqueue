import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

const schema = `
  CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'done', 'dead')),
    ready_at INTEGER NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    last_error TEXT
  ) STRICT;

  CREATE INDEX IF NOT EXISTS jobs_by_state_and_ready_time
    ON jobs (state, ready_at, id);
`;

function requireInteger(name, value, minimum) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be a safe integer >= ${minimum}`);
  }
}

export class Queue {
  #db;
  #clock;
  #statements;

  constructor(path, { clock = Date.now } = {}) {
    this.#clock = clock;
    this.#db = new DatabaseSync(path, { timeout: 5_000 });
    try {
      this.#db.exec('PRAGMA synchronous = FULL');
      this.#db.exec(schema);
      this.#statements = {
        insert: this.#db.prepare(`
          INSERT INTO jobs (id, payload, state, ready_at, attempt, max_attempts)
          VALUES (?, ?, 'queued', ?, 0, ?)
        `),
        reap: this.#db.prepare(`
          UPDATE jobs
          SET state = 'dead', last_error = 'Lease expired on final attempt'
          WHERE state = 'running' AND ready_at <= ? AND attempt >= max_attempts
        `),
        claim: this.#db.prepare(`
          UPDATE jobs
          SET state = 'running', ready_at = ?, attempt = attempt + 1
          WHERE id = (
            SELECT id FROM jobs
            WHERE state IN ('queued', 'running') AND ready_at <= ?
              AND attempt < max_attempts
            ORDER BY ready_at, id
            LIMIT 1
          )
          RETURNING id, payload, attempt, ready_at AS leaseUntil
        `),
        complete: this.#db.prepare(`
          UPDATE jobs SET state = 'done'
          WHERE id = ? AND attempt = ? AND state = 'running' AND ready_at > ?
        `),
        fail: this.#db.prepare(`
          UPDATE jobs
          SET state = CASE WHEN attempt >= max_attempts THEN 'dead' ELSE 'queued' END,
              ready_at = ?, last_error = ?
          WHERE id = ? AND attempt = ? AND state = 'running' AND ready_at > ?
        `),
        get: this.#db.prepare(`
          SELECT id, state, attempt, max_attempts AS maxAttempts, last_error AS lastError
          FROM jobs WHERE id = ?
        `),
      };
    } catch (error) {
      this.#db.close();
      throw error;
    }
  }

  #write(operation) {
    this.#db.exec('BEGIN IMMEDIATE');
    try {
      // Read time after acquiring the writer lock so lock waits cannot extend a lease.
      const result = operation(this.#clock());
      this.#db.exec('COMMIT');
      return result;
    } catch (error) {
      if (this.#db.isTransaction) this.#db.exec('ROLLBACK');
      throw error;
    }
  }

  enqueue(payload, { delayMs = 0, maxAttempts = 3 } = {}) {
    requireInteger('delayMs', delayMs, 0);
    requireInteger('maxAttempts', maxAttempts, 1);
    const id = randomUUID();
    const encoded = JSON.stringify(payload);
    if (encoded === undefined) throw new TypeError('Expected a JSON-serializable payload');

    this.#write((now) => {
      const readyAt = now + delayMs;
      requireInteger('scheduled time', readyAt, 0);
      this.#statements.insert.run(id, encoded, readyAt, maxAttempts);
    });
    return id;
  }

  claim({ leaseMs = 30_000 } = {}) {
    requireInteger('leaseMs', leaseMs, 1);
    return this.#write((now) => {
      const leaseUntil = now + leaseMs;
      requireInteger('lease expiry', leaseUntil, 1);
      this.#statements.reap.run(now);
      const [job] = this.#statements.claim.all(leaseUntil, now);
      return job ? { ...job, payload: JSON.parse(job.payload) } : null;
    });
  }

  complete({ id, attempt }) {
    return this.#write((now) => this.#statements.complete.run(id, attempt, now).changes === 1);
  }

  fail({ id, attempt }, error) {
    requireInteger('attempt', attempt, 1);
    const retryIn = Math.min(30_000, 250 * 2 ** (attempt - 1));
    const message = String(error);
    return this.#write((now) => {
      return this.#statements.fail.run(now + retryIn, message, id, attempt, now).changes === 1;
    });
  }

  get(id) {
    const job = this.#statements.get.get(id);
    return job ? { ...job } : null;
  }

  close() {
    this.#db.close();
  }
}
