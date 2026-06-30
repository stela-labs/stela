/**
 * Financial services KYC workflow.
 *
 * Demonstrates a compliance-friendly pattern: automation gathers documents,
 * validates completeness, performs deterministic checks, and waits for a human
 * analyst decision before approving or rejecting the applicant.
 */
import { workflow, StelaClient, startWorker } from "@stela/core";

interface KycInput {
  applicationId: string;
  customerId: string;
  country: string;
  requestedProducts: string[];
}

interface DocumentUploadSignal {
  documentIds: string[];
  uploadedBy: string;
}

interface AnalystDecisionSignal {
  decision: "approved" | "rejected" | "needs_more_info";
  analystId: string;
  reason?: string;
}

interface KycOutput {
  applicationId: string;
  decision: AnalystDecisionSignal["decision"];
  analystId: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestDocuments(input: KycInput): Promise<{ requestId: string }> {
  console.log(`  [kyc] requesting documents for application=${input.applicationId}`);
  return { requestId: `docreq_${input.applicationId}` };
}

async function validateDocuments(documentIds: string[]): Promise<{ complete: boolean; missing: string[] }> {
  console.log(`  [kyc] validating documents=${documentIds.join(",")}`);
  return {
    complete: documentIds.length >= 2,
    missing: documentIds.length >= 2 ? [] : ["proof_of_address"],
  };
}

async function runScreeningChecks(input: KycInput): Promise<{ riskLevel: "low" | "medium" | "high" }> {
  console.log(`  [screening] customer=${input.customerId} country=${input.country}`);
  return { riskLevel: input.country === "CH" ? "low" : "medium" };
}

async function createReviewPacket(
  input: KycInput,
  documentIds: string[],
  riskLevel: string,
): Promise<{ packetId: string }> {
  console.log(`  [case] creating review packet for application=${input.applicationId}`);
  return {
    packetId: `packet_${input.applicationId}_${documentIds.length}_${riskLevel}`,
  };
}

const kycWorkflow = workflow<KycInput, KycOutput>(
  "financial.kyc",
  async ({ input, step, sleep }) => {
    await step.run("request-documents", () => requestDocuments(input));

    const upload = await step.waitForSignal<DocumentUploadSignal>("document.uploaded", {
      timeout: "30s",
    });

    const validation = await step.run("validate-documents", () =>
      validateDocuments(upload.documentIds),
    );

    if (!validation.complete) {
      await sleep("wait-before-missing-doc-reminder", "2s");
      await step.run("send-missing-doc-reminder", async () => {
        console.log(`  [email] missing documents: ${validation.missing.join(",")}`);
      });
    }

    const screening = await step.run("screening-checks", () => runScreeningChecks(input), {
      maxAttempts: 3,
      timeoutMs: 20_000,
    });

    await step.run("create-review-packet", () =>
      createReviewPacket(input, upload.documentIds, screening.riskLevel),
    );

    const analyst = await step.waitForSignal<AnalystDecisionSignal>("analyst.decision", {
      timeout: "30s",
    });

    return {
      applicationId: input.applicationId,
      decision: analyst.decision,
      analystId: analyst.analystId,
    };
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
    kycWorkflow,
    {
      applicationId: `app_kyc_${demoId}`,
      customerId: `cus_fin_${demoId}`,
      country: "CH",
      requestedProducts: ["business_account"],
    },
    { idempotencyKey: `kyc-app-${demoId}` },
  );

  console.log(`Enqueued KYC run: ${runId}`);

  const worker = startWorker({
    connectionString,
    workflows: [kycWorkflow],
    pollIntervalMs: 250,
    logLevel: "info",
  });

  await delay(1_000);
  console.log("Sending document upload signal...");
  await client.sendSignal(runId, "document.uploaded", {
    documentIds: ["passport_front", "proof_of_address"],
    uploadedBy: "customer",
  } satisfies DocumentUploadSignal);

  await delay(1_500);
  console.log("Sending analyst decision signal...");
  await client.sendSignal(runId, "analyst.decision", {
    decision: "approved",
    analystId: "analyst_demo_001",
    reason: "Documents complete and screening risk is low.",
  } satisfies AnalystDecisionSignal);

  await delay(3_000);
  await worker.stop();
  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
