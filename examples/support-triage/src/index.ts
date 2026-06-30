/**
 * Support triage workflow.
 *
 * Demonstrates an agent-assisted support flow with account enrichment, reply
 * drafting, human review, and a durable outbound send step.
 */
import { workflow, StelaClient, startWorker } from "@stela/core";

interface TicketInput {
  ticketId: string;
  customerId: string;
  subject: string;
  body: string;
}

interface Classification {
  category: "billing" | "bug" | "how-to";
  priority: "low" | "normal" | "urgent";
  confidence: number;
}

interface AccountContext {
  plan: "free" | "pro" | "enterprise";
  openIncidents: string[];
  recentInvoices: string[];
}

interface DraftReply {
  body: string;
  internalNotes: string[];
}

interface ReviewSignal {
  approved: boolean;
  reviewerId: string;
  editedReply?: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function classifyTicket(input: TicketInput): Promise<Classification> {
  console.log(`  [agent] classifying ticket=${input.ticketId}`);
  const isBilling = /invoice|payment|billing/i.test(`${input.subject} ${input.body}`);
  return {
    category: isBilling ? "billing" : "how-to",
    priority: input.body.includes("production") ? "urgent" : "normal",
    confidence: 0.91,
  };
}

async function fetchAccountContext(customerId: string): Promise<AccountContext> {
  console.log(`  [crm] loading context for customer=${customerId}`);
  return {
    plan: "pro",
    openIncidents: [],
    recentInvoices: ["inv_2026_06"],
  };
}

async function draftSupportReply(
  ticket: TicketInput,
  classification: Classification,
  context: AccountContext,
): Promise<DraftReply> {
  console.log(`  [llm] drafting reply for ${classification.category} ticket`);
  return {
    body: [
      `Thanks for writing in about "${ticket.subject}".`,
      `I checked your ${context.plan} account context and recent activity.`,
      "The next best step is attached below for review before sending.",
    ].join("\n\n"),
    internalNotes: [
      `category=${classification.category}`,
      `priority=${classification.priority}`,
      `confidence=${classification.confidence}`,
    ],
  };
}

async function sendReply(ticketId: string, reply: string): Promise<{ messageId: string }> {
  console.log(`  [helpdesk] sending reply for ticket=${ticketId}`);
  console.log(reply);
  return { messageId: `msg_${ticketId}_${Date.now()}` };
}

const supportTriageWorkflow = workflow<TicketInput, { status: string; messageId?: string }>(
  "support.triage",
  async ({ input, step }) => {
    const classification = await step.run("classify-ticket", () =>
      classifyTicket(input),
    );

    const context = await step.run("fetch-account-context", () =>
      fetchAccountContext(input.customerId),
    );

    const draft = await step.run("draft-reply", () =>
      draftSupportReply(input, classification, context),
    );

    const review = await step.waitForSignal<ReviewSignal>("agent.reviewed", {
      timeout: "30s",
    });

    if (!review.approved) {
      return { status: `held by ${review.reviewerId}` };
    }

    const sent = await step.run("send-reviewed-reply", () =>
      sendReply(input.ticketId, review.editedReply ?? draft.body),
    );

    return { status: "sent", messageId: sent.messageId };
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
    supportTriageWorkflow,
    {
      ticketId: `ticket_${demoId}`,
      customerId: "cus_pro_123",
      subject: "Question about invoice",
      body: "Can you explain the latest invoice line item?",
    },
    { idempotencyKey: `support-ticket-${demoId}` },
  );

  console.log(`Enqueued support triage run: ${runId}`);

  const worker = startWorker({
    connectionString,
    workflows: [supportTriageWorkflow],
    pollIntervalMs: 250,
    logLevel: "info",
  });

  await delay(1_500);
  console.log("Sending reviewer decision...");
  await client.sendSignal(runId, "agent.reviewed", {
    approved: true,
    reviewerId: "support_lead_demo",
    editedReply: "Thanks for writing in. I reviewed the invoice and added the explanation below.",
  } satisfies ReviewSignal);

  await delay(3_000);
  await worker.stop();
  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
