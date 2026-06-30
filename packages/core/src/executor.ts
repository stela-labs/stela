import pg from "pg";
import {
  claimNextRun,
  completeRun,
  completeStep,
  consumeSignalAndCompleteStep,
  deadLetterRun,
  failRun,
  failStep,
  findPendingSignal,
  heartbeatRun,
  insertEvent,
  insertStep,
  loadSteps,
  scheduleStepRetry,
  sleepRun,
} from "./db.js";
import { SignalTimeoutError, SleepSignal, StepError, WaitSignal, WorkerLeaseExpiredError } from "./errors.js";
import { createRuntimeObserver } from "./observability.js";
import type { RunLease } from "./db.js";
import type {
  RegisteredWorkflowDefinition,
  RunRow,
  RuntimeLogger,
  RuntimeLogLevel,
  RuntimeMetrics,
  SignalOptions,
  StepAPI,
  StepOptions,
  StepRow,
  WorkflowContext,
  WorkflowDefinition,
} from "./types.js";
import { parseDuration } from "./duration.js";

const DEFAULT_LEASE_DURATION_MS = 30_000;

class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

/**
 * Shared options for lease management and observability, used by both
 * {@link ExecuteRunOptions} and {@link ProcessNextRunOptions}.
 */
export interface RunExecutionOptions {
  /** How long the run lease is held in milliseconds. Default: 30000. */
  leaseDurationMs?: number;
  /** How often the worker renews the run lease. Default: one third of the lease duration. */
  heartbeatIntervalMs?: number;
  /** Minimum runtime log level. Default: info. */
  logLevel?: RuntimeLogLevel;
  /** Emit default logs as structured JSON. */
  jsonLogs?: boolean;
  /** Custom runtime logger sink. */
  logger?: RuntimeLogger;
  /** Optional metrics hooks. */
  metrics?: RuntimeMetrics;
}

/** Options for a single run execution. */
export interface ExecuteRunOptions extends RunExecutionOptions {
  /** Worker id that owns the active run lease. */
  workerId: string;
}

/** Options passed to {@link processNextRun}. */
export type ProcessNextRunOptions = RunExecutionOptions;

function backoffMs(attempt: number): number {
  return Math.min(1000 * Math.pow(2, attempt), 60_000);
}

function resolvePositiveInteger(value: number | undefined, fallback: number, name: string): number {
  const resolved = value ?? fallback;
  if (!Number.isInteger(resolved) || resolved < 1) {
    throw new RangeError(`${name} must be a positive integer.`);
  }
  return resolved;
}

function resolveLeaseSettings(opts: {
  leaseDurationMs?: number;
  heartbeatIntervalMs?: number;
}): { leaseDurationMs: number; heartbeatIntervalMs: number } {
  const leaseDurationMs = resolvePositiveInteger(
    opts.leaseDurationMs,
    DEFAULT_LEASE_DURATION_MS,
    "leaseDurationMs",
  );
  if (leaseDurationMs < 2) {
    throw new RangeError("leaseDurationMs must be at least 2 milliseconds.");
  }

  const defaultHeartbeatIntervalMs = Math.max(
    1,
    Math.min(Math.floor(leaseDurationMs / 3), leaseDurationMs - 1),
  );
  const heartbeatIntervalMs = resolvePositiveInteger(
    opts.heartbeatIntervalMs,
    defaultHeartbeatIntervalMs,
    "heartbeatIntervalMs",
  );
  if (heartbeatIntervalMs >= leaseDurationMs) {
    throw new RangeError("heartbeatIntervalMs must be less than leaseDurationMs.");
  }

  return { leaseDurationMs, heartbeatIntervalMs };
}

function resolveMaxAttempts(maxAttempts: number | undefined): number {
  const resolved = maxAttempts ?? 3;
  if (!Number.isInteger(resolved) || resolved < 1) {
    throw new RangeError("Step maxAttempts must be a positive integer.");
  }
  return resolved;
}

function resolveTimeoutMs(timeoutMs: number | undefined, name: string): number | undefined {
  if (timeoutMs === undefined) return undefined;
  return resolvePositiveInteger(timeoutMs, timeoutMs, name);
}

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number | undefined,
  message: string,
): Promise<T> {
  if (timeoutMs === undefined) return await operation;

  let timer: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new TimeoutError(message)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function makeStepError(
  name: string,
  attempt: number,
  permanent: boolean,
  message: string | null | undefined,
): StepError {
  const cause = message ? new Error(message) : undefined;
  return new StepError(name, attempt, permanent, cause);
}

function startHeartbeat(
  pool: pg.Pool,
  runId: string,
  workerId: string,
  leaseDurationMs: number,
  heartbeatIntervalMs: number,
  observer = createRuntimeObserver(),
): () => void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void heartbeatRun(pool, runId, workerId, leaseDurationMs)
      .then((ok) => {
        if (!ok) {
          observer.log("warn", "run.heartbeat.lost", "Run lease heartbeat was not renewed.", {
            runId,
            workerId,
          });
          clearInterval(timer);
          return;
        }
        observer.increment("stela.run.heartbeat", { runId, workerId });
        observer.log("debug", "run.heartbeat", "Run lease heartbeat renewed.", {
          runId,
          workerId,
        });
      })
      .catch(() => {
        observer.log("warn", "run.heartbeat.error", "Run lease heartbeat failed.", {
          runId,
          workerId,
        });
        clearInterval(timer);
      })
      .finally(() => {
        inFlight = false;
      });
  }, heartbeatIntervalMs);

  return () => clearInterval(timer);
}

async function finishWithLeaseGuard(fn: () => Promise<void>): Promise<boolean> {
  try {
    await fn();
    return true;
  } catch (err) {
    if (err instanceof WorkerLeaseExpiredError) {
      return false;
    }
    throw err;
  }
}

async function buildContext<TInput>(
  pool: pg.Pool,
  run: RunRow,
  lease?: RunLease,
  observer = createRuntimeObserver(),
): Promise<WorkflowContext<TInput>> {
  const rows = await loadSteps(pool, run.id);
  const stepMap = new Map<string, StepRow>(rows.map((r) => [r.step_name, r]));

  const NO_TIMEOUT_WAKE_AT = new Date("2999-12-31T23:59:59Z");

  const step: StepAPI = {
    async waitForSignal<T>(name: string, opts?: SignalOptions): Promise<T> {
      const existing = stepMap.get(name) ?? null;

      if (existing !== null && existing.step_type !== "signal") {
        throw new Error(
          `Signal name "${name}" was previously registered as a ${existing.step_type}. Each step, sleep, and signal must have a unique name.`,
        );
      }

      if (existing?.status === "completed") {
        return existing.output as T;
      }

      const pendingSignal = await findPendingSignal(pool, run.id, name);

      if (pendingSignal) {
        let stepRow = existing;
        if (!stepRow) {
          const timeoutMs = opts?.timeout !== undefined ? parseDuration(opts.timeout) : undefined;
          const wakeAt = timeoutMs !== undefined ? new Date(Date.now() + timeoutMs) : NO_TIMEOUT_WAKE_AT;
          stepRow = await insertStep(pool, run.id, name, "signal", 1, wakeAt, lease);
          stepMap.set(name, stepRow);
        }

        await consumeSignalAndCompleteStep(pool, run.id, stepRow.id, pendingSignal.id, pendingSignal.payload, lease);
        await insertEvent(pool, run.id, "signal.received", { signalName: name });
        observer.increment("stela.signal.received", { runId: run.id, signalName: name, workflowName: run.workflow_name });
        stepMap.set(name, { ...stepRow, status: "completed", output: pendingSignal.payload });
        return pendingSignal.payload as T;
      }

      let stepRow = existing;
      if (!stepRow) {
        const timeoutMs = opts?.timeout !== undefined ? parseDuration(opts.timeout) : undefined;
        const wakeAt = timeoutMs !== undefined ? new Date(Date.now() + timeoutMs) : NO_TIMEOUT_WAKE_AT;
        stepRow = await insertStep(pool, run.id, name, "signal", 1, wakeAt, lease);
        stepMap.set(name, stepRow);
        await insertEvent(pool, run.id, "signal.waiting", { signalName: name, wakeAt });
        observer.increment("stela.signal.waiting", { runId: run.id, signalName: name, workflowName: run.workflow_name });
      }

      const wakeAt = stepRow.scheduled_at ?? NO_TIMEOUT_WAKE_AT;
      if (wakeAt <= new Date()) {
        throw new SignalTimeoutError(name);
      }

      throw new WaitSignal(name, wakeAt);
    },

    async run<T>(name: string, fn: () => Promise<T>, opts?: StepOptions): Promise<T> {
      const existing = stepMap.get(name) ?? null;

      if (existing !== null && existing.step_type !== "step") {
        throw new Error(
          `Step name "${name}" was previously registered as a sleep. Each step and sleep must have a unique name.`,
        );
      }

      if (existing?.status === "completed") {
        return existing.output as T;
      }

      if (existing?.status === "failed" && existing.attempt >= existing.max_attempts) {
        throw makeStepError(name, existing.attempt, true, existing.error);
      }

      let stepRow = existing;
      if (!stepRow) {
        const configuredMaxAttempts = resolveMaxAttempts(opts?.maxAttempts);
        stepRow = await insertStep(pool, run.id, name, "step", configuredMaxAttempts, undefined, lease);
        stepMap.set(name, stepRow);
      }

      const timeoutMs = resolveTimeoutMs(opts?.timeoutMs, "Step timeoutMs");
      const maxAttempts = stepRow.max_attempts;
      const currentAttempt = stepRow.attempt + 1;
      const stepStartedAt = Date.now();

      try {
        const result = await withTimeout(
          fn(),
          timeoutMs,
          `Step "${name}" timed out after ${timeoutMs}ms.`,
        );
        await completeStep(pool, run.id, stepRow.id, result, lease);
        await insertEvent(pool, run.id, "step.completed", { stepName: name });
        observer.increment("stela.step.completed", {
          runId: run.id,
          stepName: name,
          workflowName: run.workflow_name,
        });
        observer.timing("stela.step.duration_ms", Date.now() - stepStartedAt, {
          runId: run.id,
          stepName: name,
          workflowName: run.workflow_name,
        });
        stepMap.set(name, { ...stepRow, status: "completed", output: result });
        return result;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const cause = err instanceof Error ? err : new Error(message);
        const eventType = err instanceof TimeoutError ? "step.timeout" : "step.failed";

        if (currentAttempt >= maxAttempts) {
          await failStep(pool, run.id, stepRow.id, message, currentAttempt, lease);
          await insertEvent(pool, run.id, eventType, {
            stepName: name,
            attempt: currentAttempt,
            error: message,
          });
          observer.increment("stela.step.failed", {
            runId: run.id,
            stepName: name,
            workflowName: run.workflow_name,
          });
          observer.timing("stela.step.duration_ms", Date.now() - stepStartedAt, {
            runId: run.id,
            stepName: name,
            workflowName: run.workflow_name,
          });
          throw new StepError(name, currentAttempt, true, cause);
        }

        const scheduledAt = new Date(Date.now() + backoffMs(currentAttempt));
        await scheduleStepRetry(pool, run.id, stepRow.id, currentAttempt, scheduledAt, lease);
        await insertEvent(pool, run.id, "step.retry", {
          stepName: name,
          attempt: currentAttempt,
          error: message,
          scheduledAt,
        });
        await insertEvent(pool, run.id, "run.pending", {
          reason: "step.retry",
          stepName: name,
          scheduledAt,
        });
        observer.increment("stela.step.retry", {
          runId: run.id,
          stepName: name,
          workflowName: run.workflow_name,
        });

        throw new StepError(name, currentAttempt, false, cause);
      }
    },
  };

  const sleep = async (name: string, duration: string): Promise<void> => {
    const existing = stepMap.get(name) ?? null;

    if (existing !== null && existing.step_type !== "sleep") {
      throw new Error(
        `Sleep name "${name}" was previously registered as a step. Each step and sleep must have a unique name.`,
      );
    }

    if (existing?.status === "completed") {
      return;
    }

    const durationMs = parseDuration(duration);
    const wakeAt = existing?.scheduled_at ?? new Date(Date.now() + durationMs);

    let stepRow = existing;
    if (!stepRow) {
      stepRow = await insertStep(pool, run.id, name, "sleep", 1, wakeAt, lease);
      stepMap.set(name, stepRow);
      await insertEvent(pool, run.id, "sleep.scheduled", { stepName: name, wakeAt });
    }

    if (wakeAt > new Date()) {
      throw new SleepSignal(name, wakeAt);
    }

    if (stepRow.status !== "completed") {
      await completeStep(pool, run.id, stepRow.id, null, lease);
      stepMap.set(name, { ...stepRow, status: "completed" });
      await insertEvent(pool, run.id, "sleep.completed", { stepName: name });
    }
  };

  return { input: run.input as TInput, step, sleep };
}

/**
 * Execute a single workflow run to completion, sleep, or terminal failure.
 *
 * The run is replayed from the top of its workflow function on every call.
 * Steps with persisted results are returned from cache without re-executing.
 * Does **not** claim the run — the caller must hold a valid worker lease.
 *
 * @param pool - The connection pool.
 * @param run - The run row to execute (must be in `'running'` status).
 * @param def - The workflow definition to replay.
 * @param opts - Worker lease options for this execution.
 * @returns Resolves when the run is completed, sleeping, or failed.
 */
export async function executeRun<TInput, TOutput>(
  pool: pg.Pool,
  run: RunRow,
  def: WorkflowDefinition<TInput, TOutput>,
  opts: ExecuteRunOptions,
): Promise<void> {
  if (!opts.workerId) {
    throw new Error("executeRun requires workerId for lease-safe execution.");
  }

  const { leaseDurationMs, heartbeatIntervalMs } = resolveLeaseSettings(opts);
  const observer = createRuntimeObserver(opts);
  const runStartedAt = Date.now();
  const lease = { workerId: opts.workerId };
  if (!(await heartbeatRun(pool, run.id, lease.workerId, leaseDurationMs))) {
    observer.log("warn", "run.lease.missing", "Run lease was not valid before execution.", {
      runId: run.id,
      workerId: lease.workerId,
      workflowName: run.workflow_name,
    });
    return;
  }

  const ctx = await buildContext<TInput>(pool, run, lease, observer);
  const stopHeartbeat = startHeartbeat(
    pool,
    run.id,
    lease.workerId,
    leaseDurationMs,
    heartbeatIntervalMs,
    observer,
  );

  try {
    const workflowTimeoutMs = resolveTimeoutMs(def.opts?.timeoutMs, "Workflow timeoutMs");
    const output = await withTimeout(
      def.fn(ctx),
      workflowTimeoutMs,
      `Workflow "${def.name}" timed out after ${workflowTimeoutMs}ms.`,
    );
    await completeRun(pool, run.id, output ?? null, lease);
    await insertEvent(pool, run.id, "run.completed", { output });
    observer.increment("stela.run.completed", {
      runId: run.id,
      workflowName: run.workflow_name,
    });
    observer.timing("stela.run.duration_ms", Date.now() - runStartedAt, {
      runId: run.id,
      workflowName: run.workflow_name,
    });
    observer.log("info", "run.completed", "Run completed.", {
      runId: run.id,
      workerId: lease.workerId,
      workflowName: run.workflow_name,
    });
  } catch (err) {
    if (err instanceof WorkerLeaseExpiredError) {
      observer.log("warn", "run.lease.expired", "Run lease expired during execution.", {
        runId: run.id,
        workerId: lease.workerId,
        workflowName: run.workflow_name,
      });
      return;
    }

    if (err instanceof SleepSignal) {
      const updated = await finishWithLeaseGuard(() => sleepRun(pool, run.id, err.wakeAt, lease));
      if (updated) {
        await insertEvent(pool, run.id, "run.sleeping", {
          stepName: err.stepName,
          wakeAt: err.wakeAt,
        });
        observer.increment("stela.run.sleeping", {
          runId: run.id,
          workflowName: run.workflow_name,
        });
      }
      return;
    }

    if (err instanceof WaitSignal) {
      const updated = await finishWithLeaseGuard(() => sleepRun(pool, run.id, err.wakeAt, lease));
      if (updated) {
        await insertEvent(pool, run.id, "run.sleeping", {
          stepName: err.stepName,
          wakeAt: err.wakeAt,
          reason: "signal.waiting",
        });
        observer.increment("stela.run.sleeping", {
          runId: run.id,
          workflowName: run.workflow_name,
        });
      }
      return;
    }

    if (err instanceof StepError) {
      if (err.permanent) {
        const updated = await finishWithLeaseGuard(() =>
          deadLetterRun(pool, run.id, err.message, lease).then(() => undefined),
        );
        if (updated) {
          await insertEvent(pool, run.id, "run.dead_letter", { error: err.message });
          observer.increment("stela.run.dead_letter", {
            runId: run.id,
            workflowName: run.workflow_name,
          });
          observer.log("warn", "run.dead_letter", "Run moved to dead letter.", {
            runId: run.id,
            workerId: lease.workerId,
            workflowName: run.workflow_name,
            error: err.message,
          });
        }
      }
      return;
    }

    const message = err instanceof Error ? err.message : String(err);
    const updated = await finishWithLeaseGuard(() => failRun(pool, run.id, message, lease));
    if (updated) {
      await insertEvent(pool, run.id, err instanceof TimeoutError ? "run.timeout" : "run.failed", {
        error: message,
      });
      observer.increment(err instanceof TimeoutError ? "stela.run.timeout" : "stela.run.failed", {
        runId: run.id,
        workflowName: run.workflow_name,
      });
      observer.log("error", err instanceof TimeoutError ? "run.timeout" : "run.failed", "Run failed.", {
        runId: run.id,
        workerId: lease.workerId,
        workflowName: run.workflow_name,
        error: message,
      });
    }
  } finally {
    stopHeartbeat();
  }
}

/** @internal Result returned by {@link claimRunForWorker}. */
export interface ClaimedRunForWorker {
  /** The claimed run row. */
  run: RunRow;
  /**
   * The registered workflow definition, or `null` if no handler was found.
   * When `null`, the run has already been transitioned to `failed` and no
   * further action is required from the caller.
   */
  def: RegisteredWorkflowDefinition | null;
  /** Resolved lease duration used for this claim. */
  leaseDurationMs: number;
  /** Resolved heartbeat interval used for this claim. */
  heartbeatIntervalMs: number;
}

/**
 * Claim the next available run from the database and resolve its workflow handler.
 *
 * Emits `run.claimed` and, when recovering an expired-lease run, `run.recovered`
 * events. If no registered handler exists for the claimed workflow, the run is
 * immediately failed and `def` is returned as `null`.
 *
 * @internal Used by {@link processNextRun} and {@link startWorker}. For direct
 * use prefer {@link processNextRun}.
 *
 * @param pool - The connection pool.
 * @param workerId - Unique identifier for the calling worker.
 * @param registry - Registered workflow definitions keyed by name.
 * @param opts - Lease and observability settings.
 * @returns The claimed run and its resolved definition, or `null` if the queue is empty.
 */
export async function claimRunForWorker(
  pool: pg.Pool,
  workerId: string,
  registry: Map<string, RegisteredWorkflowDefinition>,
  opts: ProcessNextRunOptions,
): Promise<ClaimedRunForWorker | null> {
  const { leaseDurationMs, heartbeatIntervalMs } = resolveLeaseSettings(opts);
  const observer = createRuntimeObserver(opts);
  const client = await pool.connect();
  let run: RunRow | null = null;

  try {
    await client.query("BEGIN");
    run = await claimNextRun(client, workerId, leaseDurationMs);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }

  if (!run) return null;

  const claimedRun = run as RunRow & { recovered?: boolean; previous_worker_id?: string | null };
  await insertEvent(pool, run.id, "run.claimed", {
    workerId,
    recovered: claimedRun.recovered ?? false,
  });
  observer.increment("stela.run.claimed", {
    runId: run.id,
    workflowName: run.workflow_name,
    workerId,
  });
  observer.log("info", "run.claimed", "Run claimed.", {
    runId: run.id,
    workerId,
    workflowName: run.workflow_name,
    fields: { recovered: claimedRun.recovered ?? false },
  });

  if (claimedRun.recovered) {
    await insertEvent(pool, run.id, "run.recovered", {
      workerId,
      previousWorkerId: claimedRun.previous_worker_id ?? null,
    });
    observer.increment("stela.run.recovered", {
      runId: run.id,
      workflowName: run.workflow_name,
      workerId,
    });
  }

  const def = registry.get(run.workflow_name) ?? null;
  if (!def) {
    await failRun(pool, run.id, `No handler registered for workflow "${run.workflow_name}"`, {
      workerId,
    });
    await insertEvent(pool, run.id, "run.failed", {
      error: `No handler registered for workflow "${run.workflow_name}"`,
    });
    return { run, def: null, leaseDurationMs, heartbeatIntervalMs };
  }

  return { run, def, leaseDurationMs, heartbeatIntervalMs };
}

/**
 * Claim and execute one pending, sleeping, or expired running run from the database.
 *
 * Uses `FOR UPDATE SKIP LOCKED` so concurrent workers never double-claim.
 * A `try/finally` ensures the pool client is always released, even if the
 * transaction commit or rollback itself throws.
 *
 * @param pool - The connection pool.
 * @param workerId - Unique identifier for the calling worker.
 * @param registry - Registered workflow definitions keyed by name.
 * @param opts - Optional lease settings for claimed runs.
 * @returns `true` if a run was processed; `false` if the queue is empty.
 */
export async function processNextRun(
  pool: pg.Pool,
  workerId: string,
  registry: Map<string, RegisteredWorkflowDefinition>,
  opts: ProcessNextRunOptions = {},
): Promise<boolean> {
  const { leaseDurationMs, heartbeatIntervalMs } = resolveLeaseSettings(opts);
  const claimed = await claimRunForWorker(pool, workerId, registry, opts);
  if (!claimed) return false;
  if (!claimed.def) return true;

  await executeRun(pool, claimed.run, claimed.def, {
    workerId,
    leaseDurationMs,
    heartbeatIntervalMs,
    logger: opts.logger,
    logLevel: opts.logLevel,
    jsonLogs: opts.jsonLogs,
    metrics: opts.metrics,
  });
  return true;
}
