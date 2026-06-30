/**
 * Onboarding email drip campaign.
 *
 * Demonstrates durable sleeps: if the worker restarts at any point — including
 * during a multi-day sleep — the workflow resumes from exactly where it left off.
 * No cron job, no external scheduler, no lost emails.
 *
 * Run:
 *   docker compose up -d postgres
 *   DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
 *   npm run build -w examples/email-drip
 *   DATABASE_URL=postgres://stela:stela@localhost:55432/stela npm start -w examples/email-drip
 */
import { workflow, StelaClient, startWorker, DuplicateRunError } from "@stela/core";

interface OnboardingInput {
  userId: string;
  email: string;
  firstName: string;
}

interface EmailResult {
  messageId: string;
}

async function sendEmail(
  to: string,
  template: "welcome" | "getting-started" | "tips",
  firstName: string,
): Promise<EmailResult> {
  console.log(`  → [email] to=${to} template=${template} firstName=${firstName}`);
  return { messageId: `msg_${template}_${Date.now()}` };
}

async function isSubscribed(userId: string): Promise<boolean> {
  console.log(`  → [subscription] checking userId=${userId}`);
  return true;
}

const onboardingWorkflow = workflow<OnboardingInput, void>(
  "user.onboarding",
  async ({ input, step, sleep }) => {
    await step.run("send-welcome", () =>
      sendEmail(input.email, "welcome", input.firstName),
    );

    await sleep("wait-before-getting-started", "1d");

    const subscribed = await step.run("check-subscription-1", () =>
      isSubscribed(input.userId),
    );

    if (subscribed) {
      await step.run("send-getting-started", () =>
        sendEmail(input.email, "getting-started", input.firstName),
      );
    }

    await sleep("wait-before-tips", "3d");

    const stillSubscribed = await step.run("check-subscription-2", () =>
      isSubscribed(input.userId),
    );

    if (stillSubscribed) {
      await step.run("send-tips", () =>
        sendEmail(input.email, "tips", input.firstName),
      );
    }
  },
);

async function main(): Promise<void> {
  const connectionString = process.env["DATABASE_URL"];
  if (!connectionString) {
    console.error("DATABASE_URL environment variable is required.");
    process.exit(1);
  }

  const client = new StelaClient({ connectionString });

  const userId = "user_demo_001";
  try {
    const { runId } = await client.start(
      onboardingWorkflow,
      { userId, email: "demo@example.com", firstName: "Alex" },
      { idempotencyKey: `onboarding-${userId}` },
    );
    console.log(`Enqueued onboarding run: ${runId}`);
  } catch (err) {
    if (err instanceof DuplicateRunError) {
      console.log(`Onboarding already started for userId=${userId} (runId: ${err.runId})`);
    } else {
      throw err;
    }
  }

  await client.end();

  console.log("\nStarting worker...");
  const worker = startWorker({
    connectionString,
    workflows: [onboardingWorkflow],
    pollIntervalMs: 500,
    logLevel: "info",
  });

  // In this demo the workflow immediately sleeps after the welcome email.
  // The run will appear as 'sleeping' in `stela inspect`. Restart the worker
  // at any point — it will resume correctly when the sleep time passes.
  await new Promise<void>((resolve) => setTimeout(resolve, 4_000));

  await worker.stop();
  console.log("\nWorker stopped. Run `stela list` to see run state.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
