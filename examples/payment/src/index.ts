/**
 * Payment processing workflow.
 *
 * Demonstrates three things that are hard to get right without durable execution:
 *
 * 1. Idempotency — starting the workflow twice with the same order id is safe.
 *    The second call throws DuplicateRunError instead of charging the card again.
 *
 * 2. Automatic retries — transient failures (network blips, rate limits) are
 *    retried with exponential backoff. The charge step is the most critical:
 *    it retries up to 3 times before the run moves to dead_letter.
 *
 * 3. Result chaining — the charge id returned by the first step is passed
 *    durably into the fulfillment and receipt steps. If the worker crashes after
 *    charging but before sending the receipt, replay re-uses the persisted
 *    charge id rather than charging again.
 *
 * Run:
 *   docker compose up -d postgres
 *   DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
 *   npm run build -w examples/payment
 *   DATABASE_URL=postgres://stela:stela@localhost:55432/stela npm start -w examples/payment
 */
import { workflow, StelaClient, startWorker, DuplicateRunError, StepError } from "@stela/core";

interface PaymentInput {
  orderId: string;
  customerId: string;
  amountCents: number;
  currency: string;
}

interface PaymentOutput {
  orderId: string;
  chargeId: string;
  fulfillmentId: string;
}

interface ChargeResult {
  chargeId: string;
}

interface FulfillmentResult {
  fulfillmentId: string;
}

async function chargeCustomer(
  customerId: string,
  amountCents: number,
  currency: string,
  idempotencyKey: string,
): Promise<ChargeResult> {
  console.log(
    `  → [payment] charging customerId=${customerId} amount=${amountCents} ${currency} key=${idempotencyKey}`,
  );
  // Simulate occasional transient failure.
  if (Math.random() < 0.3) throw new Error("Payment gateway timeout");
  return { chargeId: `ch_${idempotencyKey.slice(-8)}_${Date.now()}` };
}

async function notifyFulfillment(
  orderId: string,
  chargeId: string,
): Promise<FulfillmentResult> {
  console.log(`  → [fulfillment] orderId=${orderId} chargeId=${chargeId}`);
  return { fulfillmentId: `ful_${orderId}_${Date.now()}` };
}

async function sendReceipt(
  customerId: string,
  orderId: string,
  chargeId: string,
  amountCents: number,
): Promise<void> {
  console.log(
    `  → [receipt] customerId=${customerId} orderId=${orderId} chargeId=${chargeId} amount=${amountCents}`,
  );
}

const paymentWorkflow = workflow<PaymentInput, PaymentOutput>(
  "payment.process",
  async ({ input, step }) => {
    // The idempotency key passed to chargeCustomer ensures that even if this
    // step is retried, the payment provider treats it as the same charge.
    const charge = await step.run(
      "charge-card",
      () =>
        chargeCustomer(
          input.customerId,
          input.amountCents,
          input.currency,
          `charge-${input.orderId}`,
        ),
      { maxAttempts: 3, timeoutMs: 15_000 },
    );

    const fulfillment = await step.run(
      "notify-fulfillment",
      () => notifyFulfillment(input.orderId, charge.chargeId),
      { maxAttempts: 3, timeoutMs: 10_000 },
    );

    await step.run(
      "send-receipt",
      () =>
        sendReceipt(
          input.customerId,
          input.orderId,
          charge.chargeId,
          input.amountCents,
        ),
      { maxAttempts: 3, timeoutMs: 10_000 },
    );

    return {
      orderId: input.orderId,
      chargeId: charge.chargeId,
      fulfillmentId: fulfillment.fulfillmentId,
    };
  },
);

async function main(): Promise<void> {
  const connectionString = process.env["DATABASE_URL"];
  if (!connectionString) {
    console.error("DATABASE_URL environment variable is required.");
    process.exit(1);
  }

  const orderId = `ord_${Date.now()}`;
  const client = new StelaClient({ connectionString });

  console.log(`Enqueuing payment for orderId=${orderId}`);
  try {
    const { runId } = await client.start(
      paymentWorkflow,
      {
        orderId,
        customerId: "cus_demo_001",
        amountCents: 4999,
        currency: "usd",
      },
      { idempotencyKey: `payment-${orderId}` },
    );
    console.log(`Run enqueued: ${runId}`);
  } catch (err) {
    if (err instanceof DuplicateRunError) {
      console.log(`Payment already exists for orderId=${orderId} (runId: ${err.runId})`);
    } else {
      throw err;
    }
  }

  await client.end();

  console.log("\nStarting worker...");
  const worker = startWorker({
    connectionString,
    workflows: [paymentWorkflow],
    pollIntervalMs: 200,
    logLevel: "info",
  });

  await new Promise<void>((resolve) => setTimeout(resolve, 8_000));
  await worker.stop();

  console.log("\nDone. Run `stela inspect <run-id>` to view step results.");
  console.log("If the run is in dead_letter (charge failed 3 times), run `stela retry <run-id>`.");
}

main().catch((err) => {
  if (err instanceof StepError) {
    console.error(`Step failed permanently: ${err.message}`);
    process.exit(1);
  }
  console.error(err);
  process.exit(1);
});
