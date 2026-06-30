import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import pg from "pg";
import { cancelRun, createPool, deadLetterRun, loadEvents, resumeRun, retryFailedRun } from "../db.js";
import { executeRun, processNextRun } from "../executor.js";
import { StelaClient } from "../client.js";
import { workflow } from "../workflow.js";
import { DuplicateRunError } from "../errors.js";
import type { RegisteredWorkflowDefinition, RunRow, StepRow, WorkflowDefinition } from "../types.js";

const DATABASE_URL = process.env["DATABASE_URL"];
const hasDb = !!DATABASE_URL;
const TEST_WORKER_ID = "test-worker";

let pool: pg.Pool;

async function queryRun(runId: string): Promise<RunRow> {
  const result = await pool.query<RunRow>(
    `SELECT * FROM stela_runs WHERE id = $1`,
    [runId],
  );
  if (!result.rows[0]) throw new Error(`Run ${runId} not found`);
  return result.rows[0];
}

async function queryStep(runId: string, stepName: string): Promise<StepRow> {
  const result = await pool.query<StepRow>(
    `SELECT * FROM stela_steps WHERE run_id = $1 AND step_name = $2`,
    [runId, stepName],
  );
  if (!result.rows[0]) throw new Error(`Step ${stepName} not found for run ${runId}`);
  return result.rows[0];
}

async function claimRunForTest(runId: string, workerId = TEST_WORKER_ID): Promise<RunRow> {
  await pool.query(
    `UPDATE stela_runs
     SET status = 'running',
         worker_id = $2,
         worker_lease_expires_at = NOW() + INTERVAL '30 seconds',
         updated_at = NOW()
     WHERE id = $1`,
    [runId, workerId],
  );
  return await queryRun(runId);
}

async function executeRunForTest<TInput, TOutput>(
  run: RunRow,
  def: WorkflowDefinition<TInput, TOutput>,
  workerId = TEST_WORKER_ID,
): Promise<void> {
  await executeRun(pool, run, def, { workerId, logLevel: "silent" });
}

beforeAll(() => {
  if (!hasDb) return;
  pool = createPool(DATABASE_URL!);
});

afterAll(async () => {
  if (pool) await pool.end();
});

beforeEach(async () => {
  if (!hasDb) return;
  await pool.query(`DELETE FROM stela_events`);
  await pool.query(`DELETE FROM stela_steps`);
  await pool.query(`DELETE FROM stela_runs`);
});

describe("executor option validation", () => {
  const run: RunRow = {
    id: "00000000-0000-0000-0000-000000000001",
    workflow_name: "test.validation",
    idempotency_key: null,
    status: "running",
    input: {},
    output: null,
    error: null,
    created_at: new Date(0),
    updated_at: new Date(0),
    scheduled_at: new Date(0),
    worker_id: "worker",
    worker_lease_expires_at: new Date(Date.now() + 30_000),
  };
  const wf = workflow("test.validation", async () => undefined);

  it("rejects heartbeat intervals that cannot renew before lease expiry", async () => {
    await expect(
      executeRun({} as pg.Pool, run, wf, {
        workerId: "worker",
        leaseDurationMs: 10,
        heartbeatIntervalMs: 10,
      }),
    ).rejects.toThrow("heartbeatIntervalMs must be less than leaseDurationMs");
  });

  it("validates processNextRun lease settings before opening a connection", async () => {
    const connect = vi.fn();
    const fakePool = { connect } as unknown as pg.Pool;

    await expect(
      processNextRun(fakePool, "worker", new Map(), {
        leaseDurationMs: 10,
        heartbeatIntervalMs: 10,
      }),
    ).rejects.toThrow("heartbeatIntervalMs must be less than leaseDurationMs");

    expect(connect).not.toHaveBeenCalled();
  });
});

describe.skipIf(!hasDb)("executor — completed steps are not re-run on replay", () => {
  it("calls the step fn only once across two executions", async () => {
    const fn = vi.fn().mockResolvedValue({ done: true });
    const wf = workflow<{ id: string }>("test.replay", async ({ input, step }) => {
      return await step.run("do-thing", fn);
    });

    const result1 = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({ id: "r1" })],
    );
    const runId = result1.rows[0].id;
    const run = await claimRunForTest(runId);

    await executeRunForTest(run, wf);
    expect(fn).toHaveBeenCalledTimes(1);

    const run2 = await claimRunForTest(runId);
    await executeRunForTest(run2, wf);
    expect(fn).toHaveBeenCalledTimes(1);
  });
});

describe.skipIf(!hasDb)("executor — crash recovery", () => {
  it("resumes correctly when a step is already completed in DB", async () => {
    const chargeCard = vi.fn(async () => ({ chargeId: "ch_test" }));
    const sendEmail = vi.fn(async () => ({ sent: true }));

    const wf = workflow<{ orderId: string }, { orderId: string; chargeId: string }>(
      "test.crash-recovery",
      async ({ input, step }) => {
        const charge = await step.run("charge", chargeCard);
        await step.run("email", sendEmail);
        return { orderId: input.orderId, chargeId: charge.chargeId };
      },
    );

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({ orderId: "ord_abc" })],
    );
    const runId = result.rows[0].id;

    await pool.query(
      `INSERT INTO stela_steps (run_id, step_name, step_type, status, output, completed_at)
       VALUES ($1, 'charge', 'step', 'completed', $2, NOW())`,
      [runId, JSON.stringify({ chargeId: "ch_from_db" })],
    );

    const run = await claimRunForTest(runId);
    await executeRunForTest(run, wf);

    expect(chargeCard).not.toHaveBeenCalled();
    expect(sendEmail).toHaveBeenCalledTimes(1);

    const finalRun = await queryRun(runId);
    expect(finalRun.status).toBe("completed");
  });
});

describe.skipIf(!hasDb)("executor — sleep pauses and resumes", () => {
  it("sets run to sleeping when sleep time has not passed", async () => {
    const emailFn = vi.fn().mockResolvedValue({ sent: true });

    const wf = workflow("test.sleep-pause", async ({ step, sleep }) => {
      await step.run("before-sleep", async () => ({ ok: true }));
      await sleep("nap", "1h");
      await step.run("after-sleep", emailFn);
    });

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;
    const run = await claimRunForTest(runId);

    await executeRunForTest(run, wf);

    const updated = await queryRun(runId);
    expect(updated.status).toBe("sleeping");
    expect(emailFn).not.toHaveBeenCalled();
  });

  it("resumes after sleep time has passed", async () => {
    const emailFn = vi.fn().mockResolvedValue({ sent: true });

    const wf = workflow("test.sleep-resume", async ({ step, sleep }) => {
      await step.run("before-sleep", async () => ({ ok: true }));
      await sleep("nap", "1h");
      await step.run("after-sleep", emailFn);
    });

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    await pool.query(
      `INSERT INTO stela_steps (run_id, step_name, step_type, status, output, scheduled_at, completed_at)
       VALUES ($1, 'before-sleep', 'step', 'completed', $2, NULL, NOW())`,
      [runId, JSON.stringify({ ok: true })],
    );
    const pastTime = new Date(Date.now() - 1000);
    await pool.query(
      `INSERT INTO stela_steps (run_id, step_name, step_type, status, scheduled_at)
       VALUES ($1, 'nap', 'sleep', 'pending', $2)`,
      [runId, pastTime],
    );

    const run = await claimRunForTest(runId);
    await executeRunForTest(run, wf);

    const finalRun = await queryRun(runId);
    expect(finalRun.status).toBe("completed");
    expect(emailFn).toHaveBeenCalledTimes(1);
  });
});

describe.skipIf(!hasDb)("executor — idempotency key", () => {
  it("prevents duplicate runs with the same idempotency key", async () => {
    const wf = workflow<{ x: number }, { x: number }>("test.idempotency", async ({ input }) => input);
    const client = new StelaClient({ connectionString: DATABASE_URL! });

    const { runId } = await client.start(wf, { x: 1 }, { idempotencyKey: "idem-1" });
    expect(runId).toBeTruthy();

    await expect(
      client.start(wf, { x: 2 }, { idempotencyKey: "idem-1" }),
    ).rejects.toThrow(DuplicateRunError);

    await client.end();
  });
});

describe.skipIf(!hasDb)("executor — retries with exponential backoff", () => {
  it("retries a failing step and eventually marks run as failed", async () => {
    let callCount = 0;
    const failingFn = vi.fn().mockImplementation(async () => {
      callCount++;
      throw new Error("step boom");
    });

    const wf = workflow("test.retries", async ({ step }) => {
      await step.run("flaky", failingFn, { maxAttempts: 3 });
    });

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    const run1 = await claimRunForTest(runId);
    await executeRunForTest(run1, wf);
    expect(failingFn).toHaveBeenCalledTimes(1);

    const afterFirst = await queryRun(runId);
    expect(afterFirst.status).toBe("pending");

    await pool.query(`UPDATE stela_runs SET scheduled_at = NOW() WHERE id = $1`, [runId]);
    const run2 = await claimRunForTest(runId);
    await executeRunForTest(run2, wf);
    expect(failingFn).toHaveBeenCalledTimes(2);

    await pool.query(`UPDATE stela_runs SET scheduled_at = NOW() WHERE id = $1`, [runId]);
    const run3 = await claimRunForTest(runId);
    await executeRunForTest(run3, wf);
    expect(failingFn).toHaveBeenCalledTimes(3);

    const finalRun = await queryRun(runId);
    expect(finalRun.status).toBe("dead_letter");
  });
});

describe.skipIf(!hasDb)("executor — timeouts", () => {
  it("retries and eventually fails a timed out step", async () => {
    const wf = workflow("test.step-timeout", async ({ step }) => {
      await step.run(
        "too-slow",
        async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 25));
          return { ok: true };
        },
        { maxAttempts: 1, timeoutMs: 1 },
      );
    });

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;
    const run = await claimRunForTest(runId);

    await executeRunForTest(run, wf);

    const finalRun = await queryRun(runId);
    const step = await queryStep(runId, "too-slow");
    const events = await loadEvents(pool, runId);

    expect(finalRun.status).toBe("dead_letter");
    expect(step.status).toBe("failed");
    expect(step.error).toContain("timed out");
    expect(events.some((event) => event.event_type === "step.timeout")).toBe(true);
    expect(events.some((event) => event.event_type === "run.dead_letter")).toBe(true);
  });

  it("fails a workflow that exceeds its timeout", async () => {
    const wf = workflow(
      "test.workflow-timeout",
      async () => {
        await new Promise<void>((resolve) => setTimeout(resolve, 25));
      },
      { timeoutMs: 1 },
    );

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;
    const run = await claimRunForTest(runId);

    await executeRunForTest(run, wf);

    const finalRun = await queryRun(runId);
    const events = await loadEvents(pool, runId);

    expect(finalRun.status).toBe("failed");
    expect(finalRun.error).toContain("timed out");
    expect(events.some((event) => event.event_type === "run.timeout")).toBe(true);
  });
});

describe.skipIf(!hasDb)("executor — expired leases are reclaimed", () => {
  it("claims and replays an expired running run", async () => {
    const fn = vi.fn(async () => ({ recovered: true }));
    const wf = workflow("test.reclaim-expired-running", async ({ step }) => {
      return await step.run("recover", fn);
    });

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (
         workflow_name,
         input,
         status,
         worker_id,
         worker_lease_expires_at
       )
       VALUES ($1, $2, 'running', 'dead-worker', NOW() - INTERVAL '1 minute')
       RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;
    const registry = new Map<string, RegisteredWorkflowDefinition>([[wf.name, wf]]);

    const didWork = await processNextRun(pool, "new-worker", registry);

    expect(didWork).toBe(true);
    expect(fn).toHaveBeenCalledTimes(1);

    const run = await queryRun(runId);
    const events = await loadEvents(pool, runId);
    expect(run.status).toBe("completed");
    expect(run.worker_id).toBeNull();
    expect(run.worker_lease_expires_at).toBeNull();
    expect(events.some((event) => event.event_type === "run.recovered")).toBe(true);
  });
});

describe.skipIf(!hasDb)("executor — cancellation", () => {
  it("cancels a claimable run so workers skip it", async () => {
    const wf = workflow("test.cancel", async () => ({ shouldNotRun: true }));
    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    await expect(cancelRun(pool, runId, "no longer needed")).resolves.toBe(true);

    const registry = new Map<string, RegisteredWorkflowDefinition>([[wf.name, wf]]);
    await expect(processNextRun(pool, "worker", registry)).resolves.toBe(false);

    const run = await queryRun(runId);
    expect(run.status).toBe("cancelled");
    expect(run.error).toBe("no longer needed");
  });
});

describe.skipIf(!hasDb)("executor — manual retry", () => {
  it("resets dead-lettered run and failed step state", async () => {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input, status, error)
       VALUES ('test.manual-retry', $1, 'dead_letter', 'step failed')
       RETURNING id`,
      [JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    await pool.query(
      `INSERT INTO stela_steps (
         run_id,
         step_name,
         step_type,
         status,
         error,
         attempt,
         max_attempts,
         completed_at
       )
       VALUES ($1, 'flaky', 'step', 'failed', 'boom', 3, 3, NOW())`,
      [runId],
    );

    await expect(retryFailedRun(pool, runId)).resolves.toBe(true);

    const run = await queryRun(runId);
    const step = await queryStep(runId, "flaky");

    expect(run.status).toBe("pending");
    expect(run.error).toBeNull();
    expect(step.status).toBe("pending");
    expect(step.error).toBeNull();
    expect(step.attempt).toBe(0);
    expect(step.completed_at).toBeNull();
  });
});

describe.skipIf(!hasDb)("executor — manual resume and dead letter", () => {
  it("resumes a sleeping run immediately", async () => {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input, status, scheduled_at)
       VALUES ('test.resume', $1, 'sleeping', NOW() + INTERVAL '1 hour')
       RETURNING id`,
      [JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    await expect(resumeRun(pool, runId)).resolves.toBe(true);

    const run = await queryRun(runId);
    expect(run.status).toBe("pending");
    expect(run.scheduled_at.getTime()).toBeLessThanOrEqual(Date.now());
  });

  it("moves an operator-paused run to dead letter", async () => {
    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input, status)
       VALUES ('test.dead-letter', $1, 'pending')
       RETURNING id`,
      [JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    await expect(deadLetterRun(pool, runId, "paused by operator")).resolves.toBe(true);

    const run = await queryRun(runId);
    expect(run.status).toBe("dead_letter");
    expect(run.error).toBe("paused by operator");
  });
});

describe.skipIf(!hasDb)("executor — concurrent worker claiming", () => {
  it("only one worker claims a single pending run when two race simultaneously", async () => {
    const fn = vi.fn(async () => ({ ok: true }));
    const wf = workflow("test.concurrent-claim", async ({ step }) => {
      return await step.run("work", fn);
    });
    const registry = new Map<string, RegisteredWorkflowDefinition>([[wf.name, wf]]);

    const result = await pool.query<{ id: string }>(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
      [wf.name, JSON.stringify({})],
    );
    const runId = result.rows[0].id;

    const [didWorkA, didWorkB] = await Promise.all([
      processNextRun(pool, "worker-a", registry, { logLevel: "silent" }),
      processNextRun(pool, "worker-b", registry, { logLevel: "silent" }),
    ]);

    expect(didWorkA || didWorkB).toBe(true);
    expect(didWorkA && didWorkB).toBe(false);

    const run = await queryRun(runId);
    expect(run.status).toBe("completed");
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("two workers each process their own run from a two-run queue", async () => {
    const fn = vi.fn(async () => ({ ok: true }));
    const wf = workflow("test.concurrent-two-runs", async ({ step }) => {
      return await step.run("work", fn);
    });
    const registry = new Map<string, RegisteredWorkflowDefinition>([[wf.name, wf]]);

    await pool.query(
      `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2), ($1, $2)`,
      [wf.name, JSON.stringify({})],
    );

    const [didWorkA, didWorkB] = await Promise.all([
      processNextRun(pool, "worker-a", registry, { logLevel: "silent" }),
      processNextRun(pool, "worker-b", registry, { logLevel: "silent" }),
    ]);

    expect(didWorkA).toBe(true);
    expect(didWorkB).toBe(true);
    expect(fn).toHaveBeenCalledTimes(2);
  });
});
