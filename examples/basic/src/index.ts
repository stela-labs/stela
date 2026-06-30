import { workflow, StelaClient, startWorker } from "@stela/core";

interface OrderInput {
  orderId: string;
}

interface ValidatedOrder {
  valid: boolean;
  orderId: string;
}

interface ChargeResult {
  chargeId: string;
}

interface EmailResult {
  sent: boolean;
}

async function validateOrder(orderId: string): Promise<ValidatedOrder> {
  console.log(`  [validateOrder] validating ${orderId}`);
  return { valid: true, orderId };
}

async function chargeCard(orderId: string): Promise<ChargeResult> {
  console.log(`  [chargeCard] charging card for ${orderId}`);
  return { chargeId: "ch_xxx" };
}

async function sendConfirmationEmail(orderId: string): Promise<EmailResult> {
  console.log(`  [sendEmail] sending confirmation for ${orderId}`);
  return { sent: true };
}

const orderWorkflow = workflow<OrderInput, { orderId: string; chargeId: string }>(
  "order.fulfill",
  async ({ input, step, sleep }) => {
    const validated = await step.run("validate-order", () =>
      validateOrder(input.orderId),
      { timeoutMs: 5_000 },
    );

    const charge = await step.run("charge-card", () =>
      chargeCard(validated.orderId),
      { maxAttempts: 3, timeoutMs: 10_000 },
    );

    await sleep("wait-before-email", "1h");

    await step.run("send-confirmation-email", () =>
      sendConfirmationEmail(input.orderId),
      { maxAttempts: 3, timeoutMs: 10_000 },
    );

    return { orderId: input.orderId, chargeId: charge.chargeId };
  },
  { timeoutMs: 60_000 },
);

async function main(): Promise<void> {
  const connectionString = process.env["DATABASE_URL"];
  if (!connectionString) {
    console.error("DATABASE_URL environment variable is required.");
    process.exit(1);
  }

  const client = new StelaClient({ connectionString });

  console.log("Enqueuing order.fulfill workflow...");
  const { runId } = await client.start(orderWorkflow, { orderId: "ord_123" });
  console.log(`Run enqueued: ${runId}`);
  await client.end();

  console.log("\nStarting worker (poll interval: 500ms)...");
  const worker = startWorker({
    connectionString,
    workflows: [orderWorkflow],
    pollIntervalMs: 500,
    logLevel: "info",
  });

  await new Promise<void>((resolve) => setTimeout(resolve, 3_000));

  console.log("\nStopping worker.");
  await worker.stop();
  console.log("Done. Run 'stela inspect <run-id>' to view the run state.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
