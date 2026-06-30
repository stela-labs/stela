/** Thrown when a referenced workflow definition is not found. */
export class WorkflowNotFoundError extends Error {
  constructor(name: string) {
    super(`Workflow not found: ${name}`);
    this.name = "WorkflowNotFoundError";
  }
}

/** Thrown when a run with the same idempotency key already exists. */
export class DuplicateRunError extends Error {
  /** The id of the pre-existing run. */
  readonly runId: string;

  /**
   * @param runId - The id of the pre-existing run.
   * @param idempotencyKey - The key that caused the conflict.
   */
  constructor(runId: string, idempotencyKey: string) {
    super(`A run with idempotency key "${idempotencyKey}" already exists (runId: ${runId})`);
    this.name = "DuplicateRunError";
    this.runId = runId;
  }
}

/**
 * Thrown when a step fails after exhausting all retry attempts,
 * or re-thrown during replay when a permanently-failed step is encountered.
 */
export class StepError extends Error {
  /** The name of the step that failed. */
  readonly stepName: string;
  /** How many attempts were made before failing. */
  readonly attempt: number;
  /**
   * True when the step has exhausted all attempts and the run should be
   * marked failed. False when the error is retryable and the run should
   * be rescheduled.
   */
  readonly permanent: boolean;

  /**
   * @param stepName - The name of the failed step.
   * @param attempt - Number of attempts made.
   * @param permanent - Whether retries are exhausted.
   * @param cause - The underlying error from the step function.
   */
  constructor(stepName: string, attempt: number, permanent: boolean, cause?: Error) {
    super(`Step "${stepName}" failed after ${attempt} attempt(s)${cause ? `: ${cause.message}` : ""}`);
    this.name = "StepError";
    this.stepName = stepName;
    this.attempt = attempt;
    this.permanent = permanent;
    if (cause) this.cause = cause;
  }
}

/** Thrown when a worker tries to update a run whose lease has expired. */
export class WorkerLeaseExpiredError extends Error {
  /** The id of the run whose lease expired. */
  readonly runId: string;

  /**
   * @param runId - The run whose lease expired.
   */
  constructor(runId: string) {
    super(`Worker lease expired for run ${runId}`);
    this.name = "WorkerLeaseExpiredError";
    this.runId = runId;
  }
}

/**
 * Thrown when `step.waitForSignal()` times out before a signal is delivered.
 * Catchable inside workflow functions to handle the timeout gracefully.
 */
export class SignalTimeoutError extends Error {
  /** The name of the signal that timed out. */
  readonly signalName: string;

  constructor(signalName: string) {
    super(`Signal "${signalName}" timed out without being received.`);
    this.name = "SignalTimeoutError";
    this.signalName = signalName;
  }
}

/**
 * Thrown by `step.waitForSignal()` to halt workflow replay until a signal arrives.
 * Caught exclusively by the executor — never surfaces to user code or error reporters.
 */
export class WaitSignal extends Error {
  /** The step name of the signal wait that triggered this. */
  readonly stepName: string;
  /** The time at which the wait times out (or far future if no timeout). */
  readonly wakeAt: Date;

  constructor(stepName: string, wakeAt: Date) {
    super(`Wait signal: "${stepName}" until ${wakeAt.toISOString()}`);
    this.name = "WaitSignal";
    this.stepName = stepName;
    this.wakeAt = wakeAt;
  }
}

/**
 * Thrown by `sleep()` to halt workflow replay until the scheduled wake time.
 * Caught exclusively by the executor — never surfaces to user code or error reporters.
 */
export class SleepSignal extends Error {
  /** The step name of the sleep that triggered this signal. */
  readonly stepName: string;
  /** The time at which the workflow should be re-claimed and resumed. */
  readonly wakeAt: Date;

  /**
   * @param stepName - The sleep step name.
   * @param wakeAt - When the workflow should resume.
   */
  constructor(stepName: string, wakeAt: Date) {
    super(`Sleep signal: "${stepName}" until ${wakeAt.toISOString()}`);
    this.name = "SleepSignal";
    this.stepName = stepName;
    this.wakeAt = wakeAt;
  }
}
