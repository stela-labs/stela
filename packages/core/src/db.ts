import pg from "pg";
import type { EventRow, RunRow, SignalRow, StepRow } from "./types.js";
import { DuplicateRunError, WorkerLeaseExpiredError } from "./errors.js";

const { Pool } = pg;

export type { Pool };

export interface RunLease {
  workerId: string;
}

export type ClaimedRunRow = RunRow & {
  recovered: boolean;
  previous_worker_id: string | null;
};

function assertLeaseUpdate(runId: string, rowCount: number | null): void {
  if (rowCount !== 1) {
    throw new WorkerLeaseExpiredError(runId);
  }
}

/** Create a new pg Pool from a connection string. */
export function createPool(connectionString: string): pg.Pool {
  return new Pool({ connectionString });
}

/**
 * Insert a new run row and return its id.
 *
 * When `idempotencyKey` is provided the insert uses `ON CONFLICT DO NOTHING`
 * so concurrent callers racing past an application-level check are handled
 * atomically. If a conflict is detected, a {@link DuplicateRunError} is thrown
 * containing the pre-existing run id.
 *
 * @param pool - The connection pool.
 * @param workflowName - The workflow name.
 * @param input - Workflow input, serialised as JSONB.
 * @param idempotencyKey - Optional deduplication key.
 * @returns The new run id.
 * @throws {DuplicateRunError} If a run with the same idempotency key already exists.
 */
export async function insertRun(
  pool: pg.Pool,
  workflowName: string,
  input: unknown,
  idempotencyKey?: string,
): Promise<string> {
  const result = await pool.query<{ id: string }>(
    `INSERT INTO stela_runs (workflow_name, input, idempotency_key)
     VALUES ($1, $2, $3)
     ON CONFLICT (workflow_name, idempotency_key) DO NOTHING
     RETURNING id`,
    [workflowName, JSON.stringify(input), idempotencyKey ?? null],
  );

  const inserted = result.rows[0];
  if (inserted) {
    return inserted.id;
  }

  if (idempotencyKey !== undefined) {
    const existing = await findRunByIdempotencyKey(pool, workflowName, idempotencyKey);
    if (existing) {
      throw new DuplicateRunError(existing.id, idempotencyKey);
    }
  }

  throw new Error(`Failed to insert workflow run for "${workflowName}".`);
}

/** Find an existing run by idempotency key. */
export async function findRunByIdempotencyKey(
  pool: pg.Pool,
  workflowName: string,
  idempotencyKey: string,
): Promise<RunRow | null> {
  const result = await pool.query<RunRow>(
    `SELECT * FROM stela_runs WHERE workflow_name = $1 AND idempotency_key = $2`,
    [workflowName, idempotencyKey],
  );
  return result.rows[0] ?? null;
}

/**
 * Claim the next available run for a worker using `FOR UPDATE SKIP LOCKED`.
 *
 * **Must be called inside an open transaction** on `client` — the caller is
 * responsible for `BEGIN` before and `COMMIT`/`ROLLBACK` after.
 *
 * @param client - A pool client with an open transaction.
 * @param workerId - Unique identifier for the claiming worker.
 * @param leaseDurationMs - How long the lease is held in milliseconds.
 * @returns The claimed run, or null if the queue is empty.
 */
export async function claimNextRun(
  client: pg.PoolClient,
  workerId: string,
  leaseDurationMs: number,
): Promise<ClaimedRunRow | null> {
  const result = await client.query<ClaimedRunRow>(
    `WITH candidate AS (
       SELECT id,
              (status = 'running') AS recovered,
              worker_id AS previous_worker_id
       FROM stela_runs
       WHERE (
           status IN ('pending', 'sleeping')
           AND scheduled_at <= NOW()
           AND (worker_lease_expires_at IS NULL OR worker_lease_expires_at < NOW())
         )
         OR (
           status = 'running'
           AND (worker_lease_expires_at IS NULL OR worker_lease_expires_at < NOW())
         )
       ORDER BY scheduled_at ASC, created_at ASC
       FOR UPDATE SKIP LOCKED
       LIMIT 1
     )
     UPDATE stela_runs
     SET status = 'running',
         worker_id = $1,
         worker_lease_expires_at = NOW() + make_interval(millisecs => $2),
         updated_at = NOW()
     FROM candidate
     WHERE stela_runs.id = candidate.id
     RETURNING stela_runs.*, candidate.recovered, candidate.previous_worker_id`,
    [workerId, leaseDurationMs],
  );
  return result.rows[0] ?? null;
}

/** Extend a running worker lease. */
export async function heartbeatRun(
  pool: pg.Pool,
  runId: string,
  workerId: string,
  leaseDurationMs: number,
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE stela_runs
     SET worker_lease_expires_at = NOW() + make_interval(millisecs => $3),
         updated_at = NOW()
     WHERE id = $1
       AND status = 'running'
       AND worker_id = $2
       AND worker_lease_expires_at > NOW()`,
    [runId, workerId, leaseDurationMs],
  );
  return result.rowCount === 1;
}

/**
 * Cancel a pending, running, or sleeping run.
 *
 * Callers are responsible for emitting a `run.cancelled` audit event after a
 * successful cancellation.
 */
export async function cancelRun(
  pool: pg.Pool,
  runId: string,
  reason = "Cancelled by operator.",
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE stela_runs
     SET status = 'cancelled',
         error = $2,
         worker_id = NULL,
         worker_lease_expires_at = NULL,
         updated_at = NOW()
     WHERE id = $1
       AND status IN ('pending', 'running', 'sleeping')`,
    [runId, reason],
  );
  return result.rowCount === 1;
}

/**
 * Resume a pending or sleeping run immediately.
 *
 * Callers are responsible for emitting a `run.resumed` audit event after a
 * successful resume.
 */
export async function resumeRun(
  pool: pg.Pool,
  runId: string,
  scheduledAt = new Date(),
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE stela_runs
     SET status = 'pending',
         scheduled_at = $2,
         worker_id = NULL,
         worker_lease_expires_at = NULL,
         updated_at = NOW()
     WHERE id = $1
       AND status IN ('pending', 'sleeping')`,
    [runId, scheduledAt],
  );
  return result.rowCount === 1;
}

/**
 * Move a run to the dead-letter state.
 *
 * When `lease` is provided the update is lease-guarded: only the owning worker
 * can transition the run, and the operation throws {@link WorkerLeaseExpiredError}
 * if the lease has expired.
 *
 * When called without a lease (operator use), callers are responsible for
 * emitting a `run.dead_letter` audit event after a successful transition.
 */
export async function deadLetterRun(
  pool: pg.Pool,
  runId: string,
  reason = "Moved to dead letter by operator.",
  lease?: RunLease,
): Promise<boolean> {
  if (lease) {
    const result = await pool.query(
      `UPDATE stela_runs
       SET status = 'dead_letter',
           error = $1,
           worker_id = NULL,
           worker_lease_expires_at = NULL,
           updated_at = NOW()
       WHERE id = $2
         AND status = 'running'
         AND worker_id = $3
         AND worker_lease_expires_at > NOW()`,
      [reason, runId, lease.workerId],
    );
    assertLeaseUpdate(runId, result.rowCount);
    return true;
  }

  const result = await pool.query(
    `UPDATE stela_runs
     SET status = 'dead_letter',
         error = $2,
         worker_id = NULL,
         worker_lease_expires_at = NULL,
         updated_at = NOW()
     WHERE id = $1
       AND status IN ('pending', 'running', 'sleeping', 'failed')`,
    [runId, reason],
  );
  return result.rowCount === 1;
}

/**
 * Retry a failed or dead-lettered run, preserving completed step outputs.
 *
 * Resets all failed steps to `pending` with a zeroed attempt counter so the
 * run gets a fresh retry budget on re-execution. Completed steps are left
 * intact and will be replayed from cache.
 */
export async function retryFailedRun(
  pool: pg.Pool,
  runId: string,
  scheduledAt = new Date(),
): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<{ id: string }>(
      `UPDATE stela_runs
       SET status = 'pending',
           output = NULL,
           error = NULL,
           scheduled_at = $2,
           worker_id = NULL,
           worker_lease_expires_at = NULL,
           updated_at = NOW()
       WHERE id = $1
         AND status IN ('failed', 'dead_letter')
       RETURNING id`,
      [runId, scheduledAt],
    );

    if (!result.rows[0]) {
      await client.query("ROLLBACK");
      return false;
    }

    await client.query(
      `UPDATE stela_steps
       SET status = 'pending',
           output = NULL,
           error = NULL,
           attempt = 0,
           completed_at = NULL
       WHERE run_id = $1
         AND status = 'failed'`,
      [runId],
    );

    await client.query("COMMIT");
    return true;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Atomically increment a step attempt and schedule its run for retry.
 *
 * Both the step and run updates are performed inside a single transaction.
 * When `lease` is provided every statement is lease-guarded; either both
 * updates succeed or the transaction is rolled back and
 * {@link WorkerLeaseExpiredError} is thrown.
 *
 * @param pool - The connection pool.
 * @param runId - The run that owns the step.
 * @param stepId - The step to retry.
 * @param attempt - The attempt number to record (the attempt that just failed).
 * @param scheduledAt - When the run should next be claimed.
 * @param lease - Optional lease guard; required during active worker execution.
 */
export async function scheduleStepRetry(
  pool: pg.Pool,
  runId: string,
  stepId: string,
  attempt: number,
  scheduledAt: Date,
  lease?: RunLease,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (lease) {
      const stepResult = await client.query(
        `UPDATE stela_steps
         SET attempt = $1, status = 'pending', error = NULL
         WHERE id = $2
           AND run_id = $3
           AND EXISTS (
             SELECT 1 FROM stela_runs
             WHERE id = $3
               AND status = 'running'
               AND worker_id = $4
               AND worker_lease_expires_at > NOW()
           )`,
        [attempt, stepId, runId, lease.workerId],
      );
      assertLeaseUpdate(runId, stepResult.rowCount);

      const runResult = await client.query(
        `UPDATE stela_runs
         SET status = 'pending', scheduled_at = $1, worker_id = NULL,
             worker_lease_expires_at = NULL, updated_at = NOW()
         WHERE id = $2
           AND status = 'running'
           AND worker_id = $3
           AND worker_lease_expires_at > NOW()`,
        [scheduledAt, runId, lease.workerId],
      );
      assertLeaseUpdate(runId, runResult.rowCount);
    } else {
      await client.query(
        `UPDATE stela_steps
         SET attempt = $1, status = 'pending', error = NULL
         WHERE id = $2 AND run_id = $3`,
        [attempt, stepId, runId],
      );
      await client.query(
        `UPDATE stela_runs
         SET status = 'pending', scheduled_at = $1, worker_id = NULL,
             worker_lease_expires_at = NULL, updated_at = NOW()
         WHERE id = $2`,
        [scheduledAt, runId],
      );
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

/** Load all steps for a run. */
export async function loadSteps(pool: pg.Pool, runId: string): Promise<StepRow[]> {
  const result = await pool.query<StepRow>(
    `SELECT * FROM stela_steps WHERE run_id = $1 ORDER BY created_at ASC`,
    [runId],
  );
  return result.rows;
}

/** Find a single step by run and name. */
export async function findStep(
  pool: pg.Pool,
  runId: string,
  stepName: string,
): Promise<StepRow | null> {
  const result = await pool.query<StepRow>(
    `SELECT * FROM stela_steps WHERE run_id = $1 AND step_name = $2`,
    [runId, stepName],
  );
  return result.rows[0] ?? null;
}

/** Insert a new step row. */
export async function insertStep(
  pool: pg.Pool,
  runId: string,
  stepName: string,
  stepType: "step" | "sleep" | "signal",
  maxAttempts: number,
  scheduledAt?: Date,
  lease?: RunLease,
): Promise<StepRow> {
  if (lease) {
    const result = await pool.query<StepRow>(
      `INSERT INTO stela_steps (run_id, step_name, step_type, max_attempts, scheduled_at)
       SELECT $1, $2, $3, $4, $5
       WHERE EXISTS (
         SELECT 1 FROM stela_runs
         WHERE id = $1
           AND status = 'running'
           AND worker_id = $6
           AND worker_lease_expires_at > NOW()
       )
       RETURNING *`,
      [runId, stepName, stepType, maxAttempts, scheduledAt ?? null, lease.workerId],
    );
    const row = result.rows[0];
    if (!row) {
      throw new WorkerLeaseExpiredError(runId);
    }
    return row;
  }

  const result = await pool.query<StepRow>(
    `INSERT INTO stela_steps (run_id, step_name, step_type, max_attempts, scheduled_at)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING *`,
    [runId, stepName, stepType, maxAttempts, scheduledAt ?? null],
  );
  return result.rows[0];
}

/** Mark a step as completed with its output. */
export async function completeStep(
  pool: pg.Pool,
  runId: string,
  stepId: string,
  output: unknown,
  lease?: RunLease,
): Promise<void> {
  if (lease) {
    const result = await pool.query(
      `UPDATE stela_steps
       SET status = 'completed', output = $1, completed_at = NOW()
       WHERE id = $2
         AND run_id = $3
         AND EXISTS (
           SELECT 1 FROM stela_runs
           WHERE id = $3
             AND status = 'running'
             AND worker_id = $4
             AND worker_lease_expires_at > NOW()
         )`,
      [JSON.stringify(output), stepId, runId, lease.workerId],
    );
    assertLeaseUpdate(runId, result.rowCount);
    return;
  }

  await pool.query(
    `UPDATE stela_steps
     SET status = 'completed', output = $1, completed_at = NOW()
     WHERE id = $2 AND run_id = $3`,
    [JSON.stringify(output), stepId, runId],
  );
}

/** Mark a step as failed and record its attempt count. */
export async function failStep(
  pool: pg.Pool,
  runId: string,
  stepId: string,
  error: string,
  attempt: number,
  lease?: RunLease,
): Promise<void> {
  if (lease) {
    const result = await pool.query(
      `UPDATE stela_steps
       SET status = 'failed', error = $1, attempt = $2, completed_at = NOW()
       WHERE id = $3
         AND run_id = $4
         AND EXISTS (
           SELECT 1 FROM stela_runs
           WHERE id = $4
             AND status = 'running'
             AND worker_id = $5
             AND worker_lease_expires_at > NOW()
         )`,
      [error, attempt, stepId, runId, lease.workerId],
    );
    assertLeaseUpdate(runId, result.rowCount);
    return;
  }

  await pool.query(
    `UPDATE stela_steps
     SET status = 'failed', error = $1, attempt = $2, completed_at = NOW()
     WHERE id = $3 AND run_id = $4`,
    [error, attempt, stepId, runId],
  );
}

/** Mark a run as sleeping, scheduled to wake at wakeAt. */
export async function sleepRun(
  pool: pg.Pool,
  runId: string,
  wakeAt: Date,
  lease?: RunLease,
): Promise<void> {
  if (lease) {
    const result = await pool.query(
      `UPDATE stela_runs
       SET status = 'sleeping', scheduled_at = $1, worker_id = NULL,
           worker_lease_expires_at = NULL, updated_at = NOW()
       WHERE id = $2
         AND status = 'running'
         AND worker_id = $3
         AND worker_lease_expires_at > NOW()`,
      [wakeAt, runId, lease.workerId],
    );
    assertLeaseUpdate(runId, result.rowCount);
    return;
  }

  await pool.query(
    `UPDATE stela_runs
     SET status = 'sleeping', scheduled_at = $1, worker_id = NULL,
         worker_lease_expires_at = NULL, updated_at = NOW()
     WHERE id = $2`,
    [wakeAt, runId],
  );
}

/** Mark a run as completed with output. */
export async function completeRun(
  pool: pg.Pool,
  runId: string,
  output: unknown,
  lease?: RunLease,
): Promise<void> {
  if (lease) {
    const result = await pool.query(
      `UPDATE stela_runs
       SET status = 'completed', output = $1, worker_id = NULL,
           worker_lease_expires_at = NULL, updated_at = NOW()
       WHERE id = $2
         AND status = 'running'
         AND worker_id = $3
         AND worker_lease_expires_at > NOW()`,
      [JSON.stringify(output), runId, lease.workerId],
    );
    assertLeaseUpdate(runId, result.rowCount);
    return;
  }

  await pool.query(
    `UPDATE stela_runs
     SET status = 'completed', output = $1, worker_id = NULL,
         worker_lease_expires_at = NULL, updated_at = NOW()
     WHERE id = $2`,
    [JSON.stringify(output), runId],
  );
}

/** Mark a run as failed. */
export async function failRun(
  pool: pg.Pool,
  runId: string,
  error: string,
  lease?: RunLease,
): Promise<void> {
  if (lease) {
    const result = await pool.query(
      `UPDATE stela_runs
       SET status = 'failed', error = $1, worker_id = NULL,
           worker_lease_expires_at = NULL, updated_at = NOW()
       WHERE id = $2
         AND status = 'running'
         AND worker_id = $3
         AND worker_lease_expires_at > NOW()`,
      [error, runId, lease.workerId],
    );
    assertLeaseUpdate(runId, result.rowCount);
    return;
  }

  await pool.query(
    `UPDATE stela_runs
     SET status = 'failed', error = $1, worker_id = NULL,
         worker_lease_expires_at = NULL, updated_at = NOW()
     WHERE id = $2`,
    [error, runId],
  );
}

/** Insert an event into the audit log. */
export async function insertEvent(
  pool: pg.Pool,
  runId: string,
  eventType: string,
  payload?: unknown,
): Promise<void> {
  await pool.query(
    `INSERT INTO stela_events (run_id, event_type, payload) VALUES ($1, $2, $3)`,
    [runId, eventType, payload !== undefined ? JSON.stringify(payload) : null],
  );
}

/** Load all events for a run in ascending order. */
export async function loadEvents(
  pool: pg.Pool,
  runId: string,
  limit = 100,
): Promise<EventRow[]> {
  const result = await pool.query<EventRow>(
    `SELECT * FROM stela_events WHERE run_id = $1 ORDER BY created_at ASC LIMIT $2`,
    [runId, limit],
  );
  return result.rows;
}

/** Load recent events across all runs, or scoped to one run. */
export async function loadRecentEvents(
  pool: pg.Pool,
  limit = 100,
  runId?: string,
): Promise<EventRow[]> {
  const result = runId
    ? await pool.query<EventRow>(
        `SELECT * FROM (
           SELECT * FROM stela_events
           WHERE run_id = $1
           ORDER BY created_at DESC
           LIMIT $2
         ) recent
         ORDER BY created_at ASC`,
        [runId, limit],
      )
    : await pool.query<EventRow>(
        `SELECT * FROM (
           SELECT * FROM stela_events
           ORDER BY created_at DESC
           LIMIT $1
         ) recent
         ORDER BY created_at ASC`,
        [limit],
      );
  return result.rows;
}

/** Load events created at or after a timestamp. */
export async function loadEventsAfter(
  pool: pg.Pool,
  since: Date,
  limit = 100,
  runId?: string,
): Promise<EventRow[]> {
  const result = runId
    ? await pool.query<EventRow>(
        `SELECT * FROM stela_events
         WHERE run_id = $1
           AND created_at >= $2
         ORDER BY created_at ASC
         LIMIT $3`,
        [runId, since, limit],
      )
    : await pool.query<EventRow>(
        `SELECT * FROM stela_events
         WHERE created_at >= $1
         ORDER BY created_at ASC
         LIMIT $2`,
        [since, limit],
      );
  return result.rows;
}

/** Fetch a run by id. */
export async function getRun(pool: pg.Pool, runId: string): Promise<RunRow | null> {
  const result = await pool.query<RunRow>(
    `SELECT * FROM stela_runs WHERE id = $1`,
    [runId],
  );
  return result.rows[0] ?? null;
}

/** List recent runs ordered by creation time descending. */
export async function listRuns(pool: pg.Pool, limit = 20): Promise<RunRow[]> {
  const result = await pool.query<RunRow>(
    `SELECT * FROM stela_runs ORDER BY created_at DESC LIMIT $1`,
    [limit],
  );
  return result.rows;
}

/** Insert a new signal row for a run. */
export async function insertSignal(
  pool: pg.Pool,
  runId: string,
  signalName: string,
  payload: unknown,
): Promise<SignalRow> {
  const result = await pool.query<SignalRow>(
    `INSERT INTO stela_signals (run_id, signal_name, payload)
     VALUES ($1, $2, $3)
     RETURNING *`,
    [runId, signalName, JSON.stringify(payload)],
  );
  return result.rows[0];
}

/**
 * Find the oldest pending (unconsumed) signal for a run.
 * Returns null if no pending signal exists.
 */
export async function findPendingSignal(
  pool: pg.Pool,
  runId: string,
  signalName: string,
): Promise<SignalRow | null> {
  const result = await pool.query<SignalRow>(
    `SELECT * FROM stela_signals
     WHERE run_id = $1 AND signal_name = $2 AND status = 'pending'
     ORDER BY created_at ASC
     LIMIT 1`,
    [runId, signalName],
  );
  return result.rows[0] ?? null;
}

/**
 * Atomically consume a pending signal and mark the corresponding step completed.
 *
 * Both updates are performed inside a single transaction. When `lease` is
 * provided the step update is lease-guarded; the transaction is rolled back
 * and {@link WorkerLeaseExpiredError} is thrown if the lease has expired.
 */
export async function consumeSignalAndCompleteStep(
  pool: pg.Pool,
  runId: string,
  stepId: string,
  signalId: string,
  payload: unknown,
  lease?: RunLease,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (lease) {
      const check = await client.query(
        `SELECT 1 FROM stela_runs
         WHERE id = $1
           AND status = 'running'
           AND worker_id = $2
           AND worker_lease_expires_at > NOW()`,
        [runId, lease.workerId],
      );
      if (!check.rows[0]) {
        throw new WorkerLeaseExpiredError(runId);
      }
    }

    await client.query(
      `UPDATE stela_signals SET status = 'consumed', consumed_at = NOW() WHERE id = $1`,
      [signalId],
    );

    await client.query(
      `UPDATE stela_steps
       SET status = 'completed', output = $1, completed_at = NOW()
       WHERE id = $2 AND run_id = $3`,
      [JSON.stringify(payload), stepId, runId],
    );

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}
