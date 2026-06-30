# Stela

[![CI](https://github.com/stela-labs/stela/actions/workflows/ci.yml/badge.svg)](https://github.com/stela-labs/stela/actions/workflows/ci.yml)

Stela is a Postgres-backed TypeScript workflow engine for durable jobs, events, retries, sleeps, and background processing.

Local-first and self-hosted, it persists workflow execution state in Postgres using a replay-based model. Steps are cached on completion; sleeps pause execution until a future time; workers resume persisted runs by replaying cached step results.

## Requirements

- Node 20+
- Postgres 14+

## Quick start

```bash
npm install
npm run build
DATABASE_URL=postgres://user:pass@localhost/stela npx stela migrate
DATABASE_URL=postgres://user:pass@localhost/stela npm start -w examples/basic
```

For local Postgres:

```bash
docker compose up -d postgres
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
```

## TypeScript Usage

A Stela app usually has three pieces:

- a shared workflow definition
- an API, route handler, or script that enqueues runs
- a worker process that executes queued runs

For a quick demo these can live in one file. In a real app, keep the workflow definition shared and run the starter and worker as separate processes.

`src/workflows/order.ts`

```ts
import { workflow } from "@stela/core";

interface OrderInput {
  orderId: string;
}

interface ShipmentSignal {
  carrier: string;
  trackingNumber: string;
}

async function chargeCard(orderId: string): Promise<{ chargeId: string }> {
  // Call Stripe, Adyen, your billing service, etc.
  return { chargeId: `ch_${orderId}` };
}

async function sendEmail(
  orderId: string,
  chargeId: string,
  shipment: ShipmentSignal,
): Promise<void> {
  // Call your email provider.
  console.log(
    `sent receipt for ${orderId} / ${chargeId} / ${shipment.trackingNumber}`,
  );
}

export const orderWorkflow = workflow<OrderInput, void>(
  "order.fulfill",
  async ({ input, step }) => {
    const payment = await step.run(
      "charge-card",
      () => chargeCard(input.orderId),
      { maxAttempts: 3, timeoutMs: 30_000 },
    );

    const shipment = await step.waitForSignal<ShipmentSignal>("shipment.ready");

    await step.run("send-email", () =>
      sendEmail(input.orderId, payment.chargeId, shipment),
    );
  },
  { timeoutMs: 120_000 },
);
```

`src/api.ts`

```ts
import { StelaClient } from "@stela/core";
import { orderWorkflow } from "./workflows/order.js";

const connectionString = process.env.DATABASE_URL!;
const client = new StelaClient({ connectionString });

const { runId } = await client.start(orderWorkflow, { orderId: "ord_123" });

console.log(`enqueued run ${runId}`);

// Later, from a webhook or internal event handler:
await client.sendSignal(runId, "shipment.ready", {
  carrier: "ups",
  trackingNumber: "1Z999AA10123456784",
});

await client.end();
```

Use `step.waitForSignal` for external events such as webhooks, approvals, document uploads, and shipment notifications. Use `sleep` for durable timers such as reminders, retry windows, dunning schedules, and delayed follow-ups.

`src/worker.ts`

```ts
import { startWorker } from "@stela/core";
import { orderWorkflow } from "./workflows/order.js";

startWorker({
  connectionString: process.env.DATABASE_URL!,
  workflows: [orderWorkflow],
  jsonLogs: true,
  logLevel: "info",
});
```

If your TypeScript project uses native Node ESM (`"type": "module"` with `moduleResolution: "NodeNext"`), local relative imports use the emitted runtime extension:

```ts
import { orderWorkflow } from "./workflows/order.js";
```

The source file is still TypeScript:

```text
src/workflows/order.ts
```

Package imports do not need a file extension:

```ts
import { workflow, StelaClient, startWorker } from "@stela/core";
```

## Single-File Demo

The runnable examples combine enqueueing and worker startup in one file so they are easy to try locally:

```ts
const { runId } = await client.start(orderWorkflow, { orderId: "ord_123" });

startWorker({
  connectionString,
  workflows: [orderWorkflow],
  jsonLogs: true,
  logLevel: "info",
});
```

That is convenient for examples. In production, run your API and worker separately.

## CLI

```
stela migrate                        Run SQL migrations in ./migrations/
stela migrate --path <dir>           Run SQL migrations from a custom directory
stela doctor                         Check database and migration health
stela worker [options]               Start a worker process
stela enqueue <workflow> <json>      Enqueue a new run
stela inspect <run-id>               Print run details and steps
stela events <run-id>                Print run events
stela tail [run-id]                  Follow run events
stela cancel <run-id> [reason]       Cancel a pending, running, or sleeping run
stela resume <run-id>                Resume a pending or sleeping run now
stela dead-letter <run-id> [reason]  Move a run to dead letter
stela retry <run-id>                 Retry a failed or dead-lettered run
stela list                           List recent runs
```

Worker options:

```bash
stela worker --json-logs --log-level info
```

## Packages

| Package | Description |
|---|---|
| `@stela/core` | SDK, executor, worker, DB layer |
| `@stela/cli` | CLI binary (`stela`) |

## Examples

Stela includes runnable examples for durable jobs, agentic workflows, and finance operations:

| Example | What it demonstrates |
|---|---|
| `examples/basic` | Order processing with durable steps and sleep |
| `examples/payment` | Idempotent payment processing with retries |
| `examples/email-drip` | Multi-step onboarding with durable sleeps |
| `examples/agent-research` | Research agent with human approval before publishing |
| `examples/support-triage` | Support ticket triage, account enrichment, reviewed reply |
| `examples/financial-kyc` | KYC document upload, screening, analyst decision |
| `examples/chargeback-evidence` | Chargeback packet generation and approved submission |
| `examples/incident-runbook` | Incident investigation with operator-approved remediation |

See [examples guide](docs/examples.md).

## Execution model

Every time a run is claimed, the workflow function is re-executed from the top (replay). `step.run` returns the cached result for completed steps without calling the function again. `sleep` stores a durable wake time and halts execution until the scheduled wake time. Workflow functions must be deterministic across replays.

See [deterministic workflow guidance](docs/determinism.md).

## Sleep durations

Supports `10s`, `30m`, `1h`, `2d` formats.

## Retries

Steps retry up to `maxAttempts` (default: 3) with exponential backoff: `min(1000 * 2^attempt, 60000)` ms.

Exhausted steps move the run to `dead_letter`. `stela retry <run-id>` moves failed or dead-lettered runs back to `pending` and resets failed step state.

## Operations

- `stela doctor` verifies database connectivity, required tables, and migration versions.
- `stela tail [run-id]` follows the durable event log.
- `stela resume <run-id>` wakes a pending or sleeping run immediately.
- `stela dead-letter <run-id> [reason]` moves a run out of active processing.

## Observability

Workers accept `logLevel`, `jsonLogs`, `logger`, and `metrics` options. Metrics hooks support `increment`, `timing`, and `gauge` without binding Stela to a metrics vendor.

## More Docs

- [Validation patterns](docs/validation.md)
- [Production deployment guide](docs/deployment.md)
- [Examples guide](docs/examples.md)

## Project

- License: MIT
- Contributing: see [CONTRIBUTING.md](CONTRIBUTING.md)
- Security: see [SECURITY.md](SECURITY.md)
