/**
 * Agent research workflow with human approval.
 *
 * Demonstrates how to put an LLM or agent loop behind durable step boundaries:
 * research, draft, policy review, wait for human approval, then publish.
 *
 * The mocked functions are where you would call search, retrieval, LLM, or
 * publishing APIs. Stela persists each step result so replay never repeats a
 * completed side effect.
 *
 * Run:
 *   docker compose up -d postgres
 *   DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
 *   npm run build -w examples/agent-research
 *   DATABASE_URL=postgres://stela:stela@localhost:55432/stela npm start -w examples/agent-research
 */
import { workflow, StelaClient, startWorker } from "@stela/core";

interface ResearchInput {
  topic: string;
  requestedBy: string;
  destination: "notion" | "github-issue" | "slack";
}

interface ResearchPlan {
  queries: string[];
  riskAreas: string[];
}

interface SourceNote {
  title: string;
  summary: string;
}

interface DraftBrief {
  title: string;
  body: string;
  citations: string[];
}

interface ApprovalSignal {
  approved: boolean;
  approverId: string;
  notes?: string;
}

interface ResearchOutput {
  status: "published" | "rejected";
  title: string;
  destination?: string;
  approverId: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function planResearch(topic: string): Promise<ResearchPlan> {
  console.log(`  [agent] planning research for topic="${topic}"`);
  return {
    queries: [
      `${topic} operational risks`,
      `${topic} implementation patterns`,
      `${topic} evaluation checklist`,
    ],
    riskAreas: ["source quality", "unsupported claims", "compliance language"],
  };
}

async function collectSources(plan: ResearchPlan): Promise<SourceNote[]> {
  console.log(`  [retrieval] collecting ${plan.queries.length} source groups`);
  return plan.queries.map((query, index) => ({
    title: `Source ${index + 1}: ${query}`,
    summary: `Relevant findings for ${query}.`,
  }));
}

async function draftBrief(topic: string, notes: SourceNote[]): Promise<DraftBrief> {
  console.log(`  [llm] drafting brief from ${notes.length} source notes`);
  return {
    title: `Research brief: ${topic}`,
    body: [
      `Topic: ${topic}`,
      "Key points:",
      ...notes.map((note) => `- ${note.summary}`),
      "Recommendation: review the evidence and approve before publishing.",
    ].join("\n"),
    citations: notes.map((note) => note.title),
  };
}

async function policyReview(draft: DraftBrief): Promise<{ passed: boolean; issues: string[] }> {
  console.log(`  [policy] reviewing draft "${draft.title}"`);
  return { passed: true, issues: [] };
}

async function publishBrief(
  destination: ResearchInput["destination"],
  draft: DraftBrief,
): Promise<{ url: string }> {
  console.log(`  [publish] destination=${destination} title="${draft.title}"`);
  return { url: `https://example.com/${destination}/research-brief` };
}

const researchWorkflow = workflow<ResearchInput, ResearchOutput>(
  "agent.research",
  async ({ input, step }) => {
    const plan = await step.run("plan-research", () => planResearch(input.topic), {
      maxAttempts: 2,
      timeoutMs: 10_000,
    });

    const notes = await step.run("collect-sources", () => collectSources(plan), {
      maxAttempts: 3,
      timeoutMs: 20_000,
    });

    const draft = await step.run("draft-brief", () => draftBrief(input.topic, notes), {
      maxAttempts: 2,
      timeoutMs: 30_000,
    });

    const review = await step.run("policy-review", () => policyReview(draft), {
      maxAttempts: 2,
      timeoutMs: 10_000,
    });

    if (!review.passed) {
      return {
        status: "rejected",
        title: draft.title,
        approverId: "policy-review",
      };
    }

    const approval = await step.waitForSignal<ApprovalSignal>("approval.received", {
      timeout: "30s",
    });

    if (!approval.approved) {
      return {
        status: "rejected",
        title: draft.title,
        approverId: approval.approverId,
      };
    }

    const publication = await step.run("publish-brief", () =>
      publishBrief(input.destination, draft),
    );

    return {
      status: "published",
      title: draft.title,
      destination: publication.url,
      approverId: approval.approverId,
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
    researchWorkflow,
    {
      topic: "durable workflows for agentic research",
      requestedBy: "pm_demo_001",
      destination: "github-issue",
    },
    { idempotencyKey: `agent-research-demo-${demoId}` },
  );

  console.log(`Enqueued agent research run: ${runId}`);

  const worker = startWorker({
    connectionString,
    workflows: [researchWorkflow],
    pollIntervalMs: 250,
    logLevel: "info",
  });

  await delay(1_500);
  console.log("Sending human approval signal...");
  await client.sendSignal(runId, "approval.received", {
    approved: true,
    approverId: "reviewer_demo_001",
    notes: "Looks ready to publish.",
  } satisfies ApprovalSignal);

  await delay(3_000);
  await worker.stop();
  await client.end();

  console.log("Done. Run `stela inspect <run-id>` to view cached agent steps.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
