/**
 * Chargeback evidence workflow.
 *
 * Demonstrates a finance operations workflow where evidence collection is
 * automated, but submission remains gated by human approval.
 */
import { workflow, StelaClient, startWorker } from "@stela/core";

interface ChargebackInput {
  disputeId: string;
  orderId: string;
  paymentId: string;
  dueInDays: number;
}

interface EvidencePacket {
  packetId: string;
  sections: string[];
  recommendation: "submit" | "write_off";
}

interface PacketApprovalSignal {
  approved: boolean;
  reviewerId: string;
  notes?: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchPaymentEvidence(paymentId: string): Promise<string[]> {
  console.log(`  [payments] fetching evidence for payment=${paymentId}`);
  return ["authorization_log", "avs_result", "capture_record"];
}

async function fetchFulfillmentEvidence(orderId: string): Promise<string[]> {
  console.log(`  [fulfillment] fetching evidence for order=${orderId}`);
  return ["tracking_number", "delivery_confirmation", "signed_receipt"];
}

async function fetchSupportHistory(orderId: string): Promise<string[]> {
  console.log(`  [support] fetching support history for order=${orderId}`);
  return ["pre_purchase_chat", "post_delivery_followup"];
}

async function generateEvidencePacket(
  input: ChargebackInput,
  sections: string[],
): Promise<EvidencePacket> {
  console.log(`  [agent] generating chargeback narrative for dispute=${input.disputeId}`);
  return {
    packetId: `packet_${input.disputeId}`,
    sections,
    recommendation: sections.length >= 6 ? "submit" : "write_off",
  };
}

async function submitEvidence(disputeId: string, packet: EvidencePacket): Promise<{ submissionId: string }> {
  console.log(`  [processor] submitting packet=${packet.packetId} dispute=${disputeId}`);
  return { submissionId: `sub_${disputeId}_${Date.now()}` };
}

const chargebackWorkflow = workflow<ChargebackInput, { status: string; submissionId?: string }>(
  "financial.chargebackEvidence",
  async ({ input, step, sleep }) => {
    const paymentEvidence = await step.run("fetch-payment-evidence", () =>
      fetchPaymentEvidence(input.paymentId),
    );

    const fulfillmentEvidence = await step.run("fetch-fulfillment-evidence", () =>
      fetchFulfillmentEvidence(input.orderId),
    );

    const supportHistory = await step.run("fetch-support-history", () =>
      fetchSupportHistory(input.orderId),
    );

    const packet = await step.run("generate-evidence-packet", () =>
      generateEvidencePacket(input, [
        ...paymentEvidence,
        ...fulfillmentEvidence,
        ...supportHistory,
      ]),
    );

    if (packet.recommendation === "write_off") {
      return { status: "write_off_recommended" };
    }

    const approval = await step.waitForSignal<PacketApprovalSignal>("packet.approved", {
      timeout: "30s",
    });

    if (!approval.approved) {
      return { status: `held_by_${approval.reviewerId}` };
    }

    await sleep("processor-submit-cooldown", "2s");

    const submission = await step.run("submit-evidence", () =>
      submitEvidence(input.disputeId, packet),
      { maxAttempts: 3, timeoutMs: 20_000 },
    );

    return { status: "submitted", submissionId: submission.submissionId };
  },
);

async function main(): Promise<void> {
  const connectionString = process.env["DATABASE_URL"];
  if (!connectionString) {
    console.error("DATABASE_URL environment variable is required.");
    process.exit(1);
  }

  const client = new StelaClient({ connectionString });
  const demoId = Date.now().toString();
  const { runId } = await client.start(
    chargebackWorkflow,
    {
      disputeId: `dp_${demoId}`,
      orderId: `ord_chargeback_${demoId}`,
      paymentId: `pay_${demoId}`,
      dueInDays: 6,
    },
    { idempotencyKey: `chargeback-dp-${demoId}` },
  );

  console.log(`Enqueued chargeback evidence run: ${runId}`);

  const worker = startWorker({
    connectionString,
    workflows: [chargebackWorkflow],
    pollIntervalMs: 250,
    logLevel: "info",
  });

  await delay(1_500);
  console.log("Sending packet approval signal...");
  await client.sendSignal(runId, "packet.approved", {
    approved: true,
    reviewerId: "finops_demo_001",
    notes: "Evidence packet is complete.",
  } satisfies PacketApprovalSignal);

  await delay(5_000);
  await worker.stop();
  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
