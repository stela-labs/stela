import { randomUUID } from "crypto";
import { createPool } from "./db.js";
import { claimRunForWorker, executeRun } from "./executor.js";
import { createRuntimeObserver } from "./observability.js";
import type { RunExecutionOptions } from "./executor.js";
import type { RegisteredWorkflowDefinition } from "./types.js";

/** Configuration for the Stela worker. */
export interface WorkerOptions {
  /** Postgres connection string. */
  connectionString: string;
  /** Workflow definitions the worker should handle. */
  workflows: RegisteredWorkflowDefinition[];
  /** How often to poll for new runs when the queue is empty, in milliseconds. Default: 1000. */
  pollIntervalMs?: number;
  /**
   * Maximum number of runs to execute concurrently within this worker process.
   *
   * A single claim loop serialises run claims while up to `concurrency` executions
   * run in parallel. Default: 1.
   */
  concurrency?: number;
  /** How long a worker owns a claimed run before heartbeat renewal. Default: 30000. */
  leaseDurationMs?: number;
  /** How often a worker renews the current run lease. Default: one third of the lease duration. */
  heartbeatIntervalMs?: number;
  /** Minimum runtime log level. Default: info. */
  logLevel?: RunExecutionOptions["logLevel"];
  /** Emit default logs as structured JSON. */
  jsonLogs?: boolean;
  /** Custom runtime logger sink. */
  logger?: RunExecutionOptions["logger"];
  /** Optional metrics hooks. */
  metrics?: RunExecutionOptions["metrics"];
}

/**
 * Start a Stela worker process that polls for pending and sleeping runs.
 *
 * A single claim loop serialises run claims while up to `opts.concurrency`
 * executions proceed in parallel. Each execution owns its own heartbeat timer
 * and lease, so concurrent runs do not interfere.
 *
 * Returns immediately with a handle. Await `handle.stop()` to drain all
 * in-flight executions and close the connection pool.
 *
 * @param opts - Worker configuration.
 * @returns A handle with a `stop` method to shut the worker down gracefully.
 */
export function startWorker(opts: WorkerOptions): { stop: () => Promise<void> } {
  if (
    opts.concurrency !== undefined &&
    (!Number.isInteger(opts.concurrency) || opts.concurrency < 1)
  ) {
    throw new RangeError("concurrency must be a positive integer.");
  }

  const pool = createPool(opts.connectionString);
  const workerId = randomUUID();
  const concurrency = opts.concurrency ?? 1;
  const pollIntervalMs = opts.pollIntervalMs ?? 1_000;

  const execOpts: RunExecutionOptions = {
    leaseDurationMs: opts.leaseDurationMs,
    heartbeatIntervalMs: opts.heartbeatIntervalMs,
    logLevel: opts.logLevel ?? "info",
    jsonLogs: opts.jsonLogs,
    logger: opts.logger,
    metrics: opts.metrics,
  };

  const observer = createRuntimeObserver(execOpts);

  const registry = new Map<string, RegisteredWorkflowDefinition>();
  for (const def of opts.workflows) {
    registry.set(def.name, def);
  }

  let stopped = false;
  let activeCount = 0;
  const activeExecutions = new Set<Promise<void>>();
  const slotFreeResolvers: Array<() => void> = [];

  let pollTimer: ReturnType<typeof setTimeout> | null = null;
  let pollTimerResolve: (() => void) | null = null;

  function waitForSlot(): Promise<void> {
    if (activeCount < concurrency) return Promise.resolve();
    return new Promise<void>((resolve) => {
      slotFreeResolvers.push(resolve);
    });
  }

  function notifySlotFree(): void {
    const resolve = slotFreeResolvers.shift();
    resolve?.();
  }

  function pollDelay(): Promise<void> {
    return new Promise<void>((resolve) => {
      pollTimerResolve = resolve;
      pollTimer = setTimeout(() => {
        pollTimer = null;
        pollTimerResolve = null;
        resolve();
      }, pollIntervalMs);
    });
  }

  function cancelPollDelay(): void {
    if (pollTimer !== null) {
      clearTimeout(pollTimer);
      pollTimer = null;
    }
    pollTimerResolve?.();
    pollTimerResolve = null;
  }

  async function claimLoop(): Promise<void> {
    while (!stopped) {
      await waitForSlot();
      if (stopped) break;

      let claimed: Awaited<ReturnType<typeof claimRunForWorker>>;
      try {
        claimed = await claimRunForWorker(pool, workerId, registry, execOpts);
      } catch (err) {
        observer.log("error", "worker.poll.error", "Worker poll failed.", {
          workerId,
          error: err instanceof Error ? err.message : String(err),
        });
        await pollDelay();
        continue;
      }

      if (!claimed) {
        await pollDelay();
        continue;
      }

      if (!claimed.def) {
        continue;
      }

      activeCount++;
      let exec: Promise<void>;
      exec = executeRun(pool, claimed.run, claimed.def, { workerId, ...execOpts }).finally(() => {
        activeCount--;
        activeExecutions.delete(exec);
        notifySlotFree();
      });
      activeExecutions.add(exec);
      void exec;
    }
  }

  const claimLoopPromise = claimLoop();
  observer.log("info", "worker.started", "Worker started.", {
    workerId,
    fields: { concurrency },
  });

  let stopPromise: Promise<void> | null = null;

  return {
    stop(): Promise<void> {
      if (stopPromise) return stopPromise;

      stopped = true;
      cancelPollDelay();

      while (slotFreeResolvers.length > 0) {
        slotFreeResolvers.shift()!();
      }

      observer.log("info", "worker.stopped", "Worker stopped.", { workerId });

      stopPromise = claimLoopPromise
        .then(() => Promise.all([...activeExecutions]))
        .then(() => pool.end())
        .catch((err) => {
          observer.log("error", "worker.close.error", "Worker pool close failed.", {
            workerId,
            error: err instanceof Error ? err.message : String(err),
          });
        })
        .then(() => undefined);

      return stopPromise;
    },
  };
}
