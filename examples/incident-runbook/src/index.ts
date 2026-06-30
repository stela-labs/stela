/**
 * Incident runbook workflow.
 *
 * Demonstrates an operations agent that investigates an alert and prepares a
 * remediation plan, but waits for explicit operator approval before taking
 * action.
 */
import { workflow, StelaClient, startWorker } from "@stela/core";

interface IncidentInput {
  incidentId: string;
  service: string;
  alertName: string;
  severity: "warning" | "critical";
}

interface Investigation {
  likelyCause: string;
  evidence: string[];
  confidence: number;
}

interface RemediationPlan {
  action: "restart-worker" | "scale-up" | "page-human";
  command: string;
  rollback: string;
}

interface OperatorApprovalSignal {
  approved: boolean;
  operatorId: string;
  reason?: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function collectTelemetry(input: IncidentInput): Promise<string[]> {
  console.log(`  [observability] collecting telemetry for service=${input.service}`);
  return [
    `${input.alertName}: p95 latency over threshold`,
    "worker queue depth rising",
    "database connections near pool limit",
  ];
}

async function investigateIncident(input: IncidentInput, telemetry: string[]): Promise<Investigation> {
  console.log(`  [agent] investigating incident=${input.incidentId}`);
  return {
    likelyCause: `${input.service} worker saturation`,
    evidence: telemetry,
    confidence: input.severity === "critical" ? 0.86 : 0.72,
  };
}

async function proposeRemediation(investigation: Investigation): Promise<RemediationPlan> {
  console.log(`  [agent] proposing remediation for cause="${investigation.likelyCause}"`);
  return {
    action: "scale-up",
    command: "kubectl scale deploy/stela-worker --replicas=4",
    rollback: "kubectl scale deploy/stela-worker --replicas=2",
  };
}

async function executeRemediation(plan: RemediationPlan): Promise<{ changeId: string }> {
  console.log(`  [ops] executing: ${plan.command}`);
  return { changeId: `change_${Date.now()}` };
}

const incidentRunbookWorkflow = workflow<IncidentInput, { status: string; changeId?: string }>(
  "ops.incidentRunbook",
  async ({ input, step }) => {
    const telemetry = await step.run("collect-telemetry", () => collectTelemetry(input), {
      maxAttempts: 3,
      timeoutMs: 20_000,
    });

    const investigation = await step.run("investigate-incident", () =>
      investigateIncident(input, telemetry),
    );

    const plan = await step.run("propose-remediation", () =>
      proposeRemediation(investigation),
    );

    const approval = await step.waitForSignal<OperatorApprovalSignal>("operator.approval", {
      timeout: "30s",
    });

    if (!approval.approved) {
      return { status: `escalated_by_${approval.operatorId}` };
    }

    const change = await step.run("execute-remediation", () =>
      executeRemediation(plan),
      { maxAttempts: 2, timeoutMs: 15_000 },
    );

    return { status: "remediated", changeId: change.changeId };
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
    incidentRunbookWorkflow,
    {
      incidentId: `inc_${demoId}`,
      service: "workflow-worker",
      alertName: "queue-depth-high",
      severity: "critical",
    },
    { idempotencyKey: `incident-inc-${demoId}` },
  );

  console.log(`Enqueued incident runbook run: ${runId}`);

  const worker = startWorker({
    connectionString,
    workflows: [incidentRunbookWorkflow],
    pollIntervalMs: 250,
    logLevel: "info",
  });

  await delay(1_500);
  console.log("Sending operator approval signal...");
  await client.sendSignal(runId, "operator.approval", {
    approved: true,
    operatorId: "sre_demo_001",
    reason: "Plan matches the runbook.",
  } satisfies OperatorApprovalSignal);

  await delay(3_000);
  await worker.stop();
  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
