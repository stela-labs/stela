import { createPool, insertEvent, insertRun, insertSignal, resumeRun } from "./db.js";
import { DuplicateRunError } from "./errors.js";
import type { WorkflowDefinition } from "./types.js";
import pg from "pg";

/** Options for starting a workflow run. */
export interface StartOptions {
  /** When provided, prevents duplicate runs for the same key. */
  idempotencyKey?: string;
}

/**
 * Client for enqueuing workflow runs into the Stela database.
 */
export class StelaClient {
  private readonly pool: pg.Pool;

  constructor(opts: { connectionString: string }) {
    this.pool = createPool(opts.connectionString);
  }

  /**
   * Enqueue a new workflow run.
   *
   * When an `idempotencyKey` is provided, the insert is performed with
   * `ON CONFLICT DO NOTHING` so concurrent callers are handled atomically.
   * A {@link DuplicateRunError} is thrown if a run with the same key already exists.
   *
   * @param def - Workflow definition created with `workflow()`.
   * @param input - Input data for the run, typed to the workflow's input type.
   * @param opts - Optional start options (idempotency key).
   * @returns Object containing the new run's id.
   * @throws {DuplicateRunError} If a run with the same idempotency key already exists.
   */
  async start<TInput, TOutput>(
    def: WorkflowDefinition<TInput, TOutput>,
    input: TInput,
    opts?: StartOptions,
  ): Promise<{ runId: string }> {
    const runId = await insertRun(this.pool, def.name, input, opts?.idempotencyKey);
    await insertEvent(this.pool, runId, "run.created", { workflowName: def.name });
    return { runId };
  }

  /**
   * Deliver a signal to a waiting workflow run.
   *
   * If the run is currently sleeping while waiting on this signal, it is
   * immediately moved to `pending` so a worker can re-claim and continue it.
   * If the run has not reached `step.waitForSignal` yet, the signal is stored
   * and consumed when the workflow gets there.
   *
   * @param runId - The run to signal.
   * @param signalName - Must match the name passed to `step.waitForSignal`.
   * @param payload - Data delivered to the workflow as the signal return value.
   */
  async sendSignal(runId: string, signalName: string, payload?: unknown): Promise<void> {
    await insertSignal(this.pool, runId, signalName, payload ?? null);
    await resumeRun(this.pool, runId);
    await insertEvent(this.pool, runId, "signal.sent", { signalName, payload: payload ?? null });
  }

  /** Release the underlying connection pool. */
  async end(): Promise<void> {
    await this.pool.end();
  }
}
