# Testing

## Running the test suite

Stela's DB-backed tests require a running Postgres instance. The fastest path is the included Docker Compose:

```bash
docker compose up -d postgres
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npm test
```

Tests that require a database are gated with `describe.skipIf(!hasDb)` and are silently skipped without `DATABASE_URL`. The two option-validation tests always run.

## Testing your own workflows

### Unit testing step logic

Step functions are plain async functions. Test them independently without Stela or a database:

```ts
import { describe, expect, it } from "vitest";

async function chargeCard(customerId: string, amountCents: number): Promise<{ chargeId: string }> {
  // your implementation
}

describe("chargeCard", () => {
  it("returns a charge id", async () => {
    const result = await chargeCard("cus_123", 5000);
    expect(result.chargeId).toMatch(/^ch_/);
  });
});
```

Keep business logic in ordinary functions that step closures call. That keeps the logic unit-testable and the workflow itself thin.

### Integration testing a full workflow

For end-to-end workflow tests you need a real database. Use the same `DATABASE_URL` pattern:

```ts
import { beforeAll, afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import { createPool, executeRun, loadEvents, loadSteps } from "@stela/core";
import { workflow } from "@stela/core";
import type { RunRow } from "@stela/core";
import pg from "pg";

const DATABASE_URL = process.env["DATABASE_URL"];
const hasDb = !!DATABASE_URL;

let pool: pg.Pool;

beforeAll(() => {
  if (!hasDb) return;
  pool = createPool(DATABASE_URL!);
});

afterAll(async () => {
  if (pool) await pool.end();
});

beforeEach(async () => {
  if (!hasDb) return;
  await pool.query("DELETE FROM stela_events");
  await pool.query("DELETE FROM stela_steps");
  await pool.query("DELETE FROM stela_runs");
});

async function insertAndClaim(workflowName: string, input: unknown): Promise<RunRow> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
    [workflowName, JSON.stringify(input)],
  );
  const runId = rows[0].id;
  await pool.query(
    `UPDATE stela_runs
     SET status = 'running', worker_id = 'test-worker',
         worker_lease_expires_at = NOW() + INTERVAL '30 seconds'
     WHERE id = $1`,
    [runId],
  );
  const { rows: runRows } = await pool.query<RunRow>(`SELECT * FROM stela_runs WHERE id = $1`, [runId]);
  return runRows[0];
}

describe.skipIf(!hasDb)("orderWorkflow", () => {
  it("completes when all steps succeed", async () => {
    const chargeCard = vi.fn(async () => ({ chargeId: "ch_test" }));
    const sendReceipt = vi.fn(async () => ({ sent: true }));

    const wf = workflow<{ orderId: string }>("order.fulfill", async ({ input, step }) => {
      const charge = await step.run("charge", () => chargeCard(input.orderId));
      await step.run("receipt", () => sendReceipt(charge.chargeId));
      return { chargeId: charge.chargeId };
    });

    const run = await insertAndClaim(wf.name, { orderId: "ord_abc" });
    await executeRun(pool, run, wf, { workerId: "test-worker", logLevel: "silent" });

    const { rows: [final] } = await pool.query<RunRow>(
      `SELECT * FROM stela_runs WHERE id = $1`, [run.id],
    );
    expect(final.status).toBe("completed");
    expect(chargeCard).toHaveBeenCalledTimes(1);
    expect(sendReceipt).toHaveBeenCalledTimes(1);
  });
});
```

### Testing retry behaviour

Simulate a flaky step by controlling when `vi.fn()` starts succeeding:

```ts
it("retries a flaky step and completes", async () => {
  let calls = 0;
  const flaky = vi.fn(async () => {
    calls++;
    if (calls < 3) throw new Error("transient error");
    return { ok: true };
  });

  const wf = workflow("retry.test", async ({ step }) => {
    await step.run("flaky-step", flaky, { maxAttempts: 3 });
  });

  // First execution — step fails, run returns to pending
  const run1 = await insertAndClaim(wf.name, {});
  await executeRun(pool, run1, wf, { workerId: "test-worker", logLevel: "silent" });
  await pool.query(`UPDATE stela_runs SET scheduled_at = NOW() WHERE id = $1`, [run1.id]);

  // Second execution — step fails again
  const run2 = await insertAndClaim(run1.id, {});   // re-claim by id
  await executeRun(pool, run2, wf, { workerId: "test-worker", logLevel: "silent" });
  await pool.query(`UPDATE stela_runs SET scheduled_at = NOW() WHERE id = $1`, [run1.id]);

  // Third execution — step succeeds
  const run3 = await insertAndClaim(run1.id, {});
  await executeRun(pool, run3, wf, { workerId: "test-worker", logLevel: "silent" });

  const { rows: [final] } = await pool.query<RunRow>(
    `SELECT * FROM stela_runs WHERE id = $1`, [run1.id],
  );
  expect(final.status).toBe("completed");
  expect(flaky).toHaveBeenCalledTimes(3);
});
```

### Testing sleep behaviour

Insert the sleep step row with a past `scheduled_at` to simulate the wake time having passed:

```ts
it("resumes after a sleep", async () => {
  const afterSleep = vi.fn(async () => ({ sent: true }));

  const wf = workflow("sleep.test", async ({ step, sleep }) => {
    await sleep("pause", "7d");
    await step.run("post-sleep", afterSleep);
  });

  const { rows: [{ id: runId }] } = await pool.query<{ id: string }>(
    `INSERT INTO stela_runs (workflow_name, input) VALUES ($1, $2) RETURNING id`,
    [wf.name, JSON.stringify({})],
  );

  // Insert a sleep step already past its wake time
  await pool.query(
    `INSERT INTO stela_steps (run_id, step_name, step_type, status, scheduled_at)
     VALUES ($1, 'pause', 'sleep', 'pending', NOW() - INTERVAL '1 second')`,
    [runId],
  );

  const run = await insertAndClaim(runId, {});  // re-use the same runId
  await executeRun(pool, run, wf, { workerId: "test-worker", logLevel: "silent" });

  expect(afterSleep).toHaveBeenCalledTimes(1);
});
```

### Testing crash recovery

Set a run to `running` with an already-expired lease to verify it is re-claimed:

```ts
it("re-claims an expired run", async () => {
  // Insert a run that looks like it was abandoned mid-execution
  await pool.query(
    `INSERT INTO stela_runs (workflow_name, input, status, worker_id, worker_lease_expires_at)
     VALUES ($1, $2, 'running', 'crashed-worker', NOW() - INTERVAL '1 minute')`,
    [wf.name, JSON.stringify({})],
  );

  const didWork = await processNextRun(pool, "new-worker", registry, { logLevel: "silent" });
  expect(didWork).toBe(true);
  // assert run completed, events include run.recovered
});
```

## What to always test

For any workflow you put in production, cover at least:

- **Happy path** — all steps succeed, run reaches `completed`.
- **Step retry** — one step fails transiently, run eventually completes.
- **Permanent failure** — step exhausts attempts, run reaches `dead_letter`.
- **Sleep resume** — workflow pauses, resumes after sleep time passes.
- **Idempotency** — starting the same run twice with the same key throws `DuplicateRunError`.
- **Crash recovery** — expired-lease run is re-claimed and replays correctly.
