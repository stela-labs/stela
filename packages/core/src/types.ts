/** Options for a single step. */
export interface StepOptions {
  /** Maximum number of execution attempts before marking as failed. Default: 3. */
  maxAttempts?: number;
  /** Maximum wall-clock time for one step attempt in milliseconds. */
  timeoutMs?: number;
}

/** Options for waiting on a signal. */
export interface SignalOptions {
  /**
   * How long to wait for the signal before throwing {@link SignalTimeoutError}.
   * Accepts the same duration format as `sleep` (e.g. `"1h"`, `"30m"`).
   * If omitted the workflow waits indefinitely until a signal is delivered.
   */
  timeout?: string;
}

/** API surface provided to a workflow function for executing steps. */
export interface StepAPI {
  /** Run a named step, caching the result so replays skip re-execution. */
  run<T>(name: string, fn: () => Promise<T>, opts?: StepOptions): Promise<T>;
  /**
   * Pause the workflow until a signal with the given name is delivered via
   * `StelaClient.sendSignal`, then return the signal payload.
   *
   * On replay the cached payload is returned without re-waiting.
   * If `opts.timeout` is set and expires before a signal arrives,
   * {@link SignalTimeoutError} is thrown (catchable inside the workflow).
   */
  waitForSignal<T = unknown>(name: string, opts?: SignalOptions): Promise<T>;
}

/** Context object passed to every workflow function invocation. */
export interface WorkflowContext<TInput> {
  /** The input provided when the run was started. */
  input: TInput;
  /** Step execution API. */
  step: StepAPI;
  /** Pause the workflow for the given duration. */
  sleep: (name: string, duration: string) => Promise<void>;
}

/** Options for a workflow definition. */
export interface WorkflowOptions {
  /** Maximum wall-clock time for one workflow execution attempt in milliseconds. */
  timeoutMs?: number;
}

/** Logging levels accepted by the runtime logger. */
export type RuntimeLogLevel = "debug" | "info" | "warn" | "error" | "silent";

/** Structured runtime log event emitted by workers and executors. */
export interface RuntimeLogEvent {
  level: Exclude<RuntimeLogLevel, "silent">;
  event: string;
  message: string;
  timestamp: string;
  runId?: string;
  workerId?: string;
  workflowName?: string;
  error?: string;
  fields?: Record<string, unknown>;
}

/** Runtime logger sink. */
export type RuntimeLogger = (event: RuntimeLogEvent) => void;

/** Runtime metrics hooks. */
export interface RuntimeMetrics {
  /** Increment a counter by one. */
  increment?: (name: string, tags?: Record<string, string>) => void;
  /** Record a duration in milliseconds. */
  timing?: (name: string, valueMs: number, tags?: Record<string, string>) => void;
  /** Record a point-in-time numeric value. */
  gauge?: (name: string, value: number, tags?: Record<string, string>) => void;
}

/** A registered workflow definition. */
export interface WorkflowDefinition<TInput = unknown, TOutput = void> {
  /** Unique workflow name used for persistence and lookup. */
  name: string;
  /** The workflow function to execute. */
  fn: (ctx: WorkflowContext<TInput>) => Promise<TOutput>;
  /** Optional workflow execution settings. */
  opts?: WorkflowOptions;
}

/** A workflow definition accepted by the worker registry. */
export type RegisteredWorkflowDefinition = WorkflowDefinition<never, unknown>;

/** Current lifecycle state of a workflow run. */
export type RunStatus =
  | "pending"
  | "running"
  | "sleeping"
  | "completed"
  | "failed"
  | "dead_letter"
  | "cancelled";

/** A row from stela_runs. */
export interface RunRow {
  id: string;
  workflow_name: string;
  idempotency_key: string | null;
  status: RunStatus;
  input: unknown;
  output: unknown;
  error: string | null;
  created_at: Date;
  updated_at: Date;
  scheduled_at: Date;
  worker_id: string | null;
  worker_lease_expires_at: Date | null;
}

/** A row from stela_steps. */
export interface StepRow {
  id: string;
  run_id: string;
  step_name: string;
  step_type: "step" | "sleep" | "signal";
  status: "pending" | "completed" | "failed";
  output: unknown;
  error: string | null;
  attempt: number;
  max_attempts: number;
  scheduled_at: Date | null;
  created_at: Date;
  completed_at: Date | null;
}

/** A row from stela_signals. */
export interface SignalRow {
  id: string;
  run_id: string;
  signal_name: string;
  payload: unknown;
  status: "pending" | "consumed";
  created_at: Date;
  consumed_at: Date | null;
}

/** A row from stela_events. */
export interface EventRow {
  id: string;
  run_id: string;
  event_type: string;
  payload: unknown;
  created_at: Date;
}
