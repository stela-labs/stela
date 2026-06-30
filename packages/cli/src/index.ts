#!/usr/bin/env node
import { readdir, readFile } from "fs/promises";
import { join, resolve } from "path";
import pg from "pg";
import {
  cancelRun,
  createPool,
  deadLetterRun,
  getRun,
  insertRun,
  insertEvent,
  listRuns,
  loadEventsAfter,
  loadEvents,
  loadRecentEvents,
  loadSteps,
  resumeRun,
  retryFailedRun,
} from "@stela/core";

function getDatabaseUrl(): string {
  const url = process.env["DATABASE_URL"];
  if (!url) {
    console.error("Error: DATABASE_URL environment variable is required.");
    process.exit(1);
  }
  return url;
}

async function withPool<T>(fn: (pool: pg.Pool) => Promise<T>): Promise<T> {
  const pool = createPool(getDatabaseUrl());
  try {
    return await fn(pool);
  } finally {
    await pool.end();
  }
}

function formatDate(date: Date | null | undefined): string {
  return date ? date.toISOString() : "(none)";
}

function formatDurationMs(ms: number | null): string {
  if (ms === null) return "(none)";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60_000).toFixed(2)}m`;
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function printEventLine(event: {
  id: string;
  run_id: string;
  event_type: string;
  payload: unknown;
  created_at: Date;
}): void {
  console.log(`${event.created_at.toISOString()} ${event.run_id} ${event.event_type}`);
  if (event.payload !== null && event.payload !== undefined) {
    console.log(`  ${formatJson(event.payload)}`);
  }
}

async function ensureMigrationsTable(pool: pg.Pool): Promise<void> {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS stela_migrations (
       version VARCHAR(255) PRIMARY KEY,
       name VARCHAR(255) NOT NULL,
       applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
     )`,
  );
}

async function loadAppliedMigrations(pool: pg.Pool): Promise<Set<string>> {
  const result = await pool.query<{ version: string }>(
    `SELECT version FROM stela_migrations`,
  );
  return new Set(result.rows.map((row) => row.version));
}

async function applyMigration(pool: pg.Pool, file: string, sql: string): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(sql);
    await client.query(
      `INSERT INTO stela_migrations (version, name) VALUES ($1, $2)
       ON CONFLICT (version) DO NOTHING`,
      [file, file],
    );
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

async function cmdMigrate(migrationsPath?: string): Promise<void> {
  const migrationsDir = resolve(process.cwd(), migrationsPath ?? "migrations");
  let files: string[];
  try {
    files = (await readdir(migrationsDir))
      .filter((f) => f.endsWith(".sql"))
      .sort();
  } catch {
    console.error(`Error: could not read migrations directory at ${migrationsDir}`);
    process.exit(1);
  }

  await withPool(async (pool) => {
    await ensureMigrationsTable(pool);
    const applied = await loadAppliedMigrations(pool);
    let appliedCount = 0;

    for (const file of files) {
      if (applied.has(file)) continue;

      const filePath = join(migrationsDir, file);
      const sql = await readFile(filePath, "utf-8");
      console.log(`Running migration: ${file}`);
      await applyMigration(pool, file, sql);
      console.log(`  done.`);
      appliedCount++;
    }

    if (appliedCount === 0) {
      console.log("No pending migrations.");
    }
  });

  console.log("All migrations applied.");
}

async function cmdWorker(_args: string[]): Promise<void> {
  console.error(
    "Error: 'stela worker' cannot register workflow handlers.\n" +
    "\n" +
    "  Workers must be started programmatically so your workflow functions are in scope:\n" +
    "\n" +
    "    import { startWorker } from '@stela/core';\n" +
    "    startWorker({ connectionString, workflows: [myWorkflow] });\n" +
    "\n" +
    "  See the README for a complete example.",
  );
  process.exit(1);
}

async function cmdEnqueue(workflowName: string, jsonInput: string): Promise<void> {
  let input: unknown;
  try {
    input = JSON.parse(jsonInput);
  } catch {
    console.error("Error: <json-input> must be valid JSON.");
    process.exit(1);
  }

  await withPool(async (pool) => {
    const runId = await insertRun(pool, workflowName, input);
    await insertEvent(pool, runId, "run.created", { workflowName });
    console.log(`Enqueued run ${runId} for workflow "${workflowName}".`);
  });
}

async function cmdInspect(runId: string): Promise<void> {
  await withPool(async (pool) => {
    const run = await getRun(pool, runId);
    if (!run) {
      console.error(`Run not found: ${runId}`);
      process.exit(1);
    }

    console.log("=== Run ===");
    console.log(`ID:           ${run.id}`);
    console.log(`Workflow:     ${run.workflow_name}`);
    console.log(`Status:       ${run.status}`);
    console.log(`Idempotency:  ${run.idempotency_key ?? "(none)"}`);
    console.log(`Worker:       ${run.worker_id ?? "(none)"}`);
    console.log(`Lease:        ${formatDate(run.worker_lease_expires_at)}`);
    console.log(`Created:      ${formatDate(run.created_at)}`);
    console.log(`Updated:      ${formatDate(run.updated_at)}`);
    console.log(`Duration:     ${formatDurationMs(run.updated_at.getTime() - run.created_at.getTime())}`);
    console.log(`Scheduled at: ${formatDate(run.scheduled_at)}`);
    console.log(`Input:        ${formatJson(run.input)}`);
    if (run.output !== null && run.output !== undefined) {
      console.log(`Output:       ${formatJson(run.output)}`);
    }
    if (run.error) {
      console.log(`Error:        ${run.error}`);
    }

    const steps = await loadSteps(pool, runId);
    if (steps.length > 0) {
      console.log("\n=== Steps ===");
      for (const step of steps) {
        const duration =
          step.completed_at === null ? null : step.completed_at.getTime() - step.created_at.getTime();
        console.log(
          `  [${step.step_type.padEnd(5)}] ${step.step_name.padEnd(30)} ${step.status}`,
        );
        console.log(`          attempts: ${step.attempt}/${step.max_attempts}`);
        console.log(`          created:  ${formatDate(step.created_at)}`);
        console.log(`          scheduled: ${formatDate(step.scheduled_at)}`);
        console.log(`          completed: ${formatDate(step.completed_at)}`);
        console.log(`          duration: ${formatDurationMs(duration)}`);
        if (step.output !== null && step.output !== undefined) {
          console.log(`          output:   ${formatJson(step.output)}`);
        }
        if (step.error) {
          console.log(`          error:    ${step.error}`);
        }
      }
    }
  });
}

async function cmdEvents(runId: string): Promise<void> {
  await withPool(async (pool) => {
    const run = await getRun(pool, runId);
    if (!run) {
      console.error(`Run not found: ${runId}`);
      process.exit(1);
    }

    const limit = 100;
    const events = await loadEvents(pool, runId, limit);
    if (events.length === 0) {
      console.log("No events found.");
      return;
    }

    console.log("=== Events ===");
    for (const event of events) {
      printEventLine(event);
    }

    if (events.length === limit) {
      console.warn(`\nWarning: output limited to ${limit} events. Use the API with a higher limit to retrieve the full history.`);
    }
  });
}

async function cmdTail(runId?: string): Promise<void> {
  let stopping = false;
  process.once("SIGINT", () => {
    stopping = true;
  });
  process.once("SIGTERM", () => {
    stopping = true;
  });

  await withPool(async (pool) => {
    if (runId) {
      const run = await getRun(pool, runId);
      if (!run) {
        console.error(`Run not found: ${runId}`);
        process.exit(1);
      }
    }

    const seen = new Set<string>();
    let cursor = new Date();
    const initialEvents = await loadRecentEvents(pool, 20, runId);
    if (initialEvents.length > 0) {
      for (const event of initialEvents) {
        seen.add(event.id);
        printEventLine(event);
      }
      cursor = initialEvents[initialEvents.length - 1].created_at;
    }

    while (!stopping) {
      await sleepMs(1_000);
      const events = await loadEventsAfter(pool, cursor, 100, runId);
      for (const event of events) {
        if (seen.has(event.id)) continue;
        seen.add(event.id);
        printEventLine(event);
        if (event.created_at > cursor) {
          cursor = event.created_at;
        }
      }
    }
  });
}

async function cmdCancel(runId: string, reason?: string): Promise<void> {
  await withPool(async (pool) => {
    const run = await getRun(pool, runId);
    if (!run) {
      console.error(`Run not found: ${runId}`);
      process.exit(1);
    }

    const cancelReason = reason ?? "Cancelled by operator.";
    const cancelled = await cancelRun(pool, runId, cancelReason);
    if (!cancelled) {
      console.error(`Run is not cancellable: ${runId} has status ${run.status}`);
      process.exit(1);
    }

    await insertEvent(pool, runId, "run.cancelled", { reason: cancelReason });
    console.log(`Cancelled run ${runId}.`);
  });
}

async function cmdResume(runId: string): Promise<void> {
  await withPool(async (pool) => {
    const run = await getRun(pool, runId);
    if (!run) {
      console.error(`Run not found: ${runId}`);
      process.exit(1);
    }

    const resumed = await resumeRun(pool, runId);
    if (!resumed) {
      console.error(`Run is not resumable: ${runId} has status ${run.status}`);
      process.exit(1);
    }

    await insertEvent(pool, runId, "run.resumed", {});
    console.log(`Resumed run ${runId}.`);
  });
}

async function cmdDeadLetter(runId: string, reason?: string): Promise<void> {
  await withPool(async (pool) => {
    const run = await getRun(pool, runId);
    if (!run) {
      console.error(`Run not found: ${runId}`);
      process.exit(1);
    }

    const deadLetterReason = reason ?? "Moved to dead letter by operator.";
    const moved = await deadLetterRun(pool, runId, deadLetterReason);
    if (!moved) {
      console.error(`Run cannot be moved to dead letter: ${runId} has status ${run.status}`);
      process.exit(1);
    }

    await insertEvent(pool, runId, "run.dead_letter.manual", { reason: deadLetterReason });
    console.log(`Moved run ${runId} to dead letter.`);
  });
}

async function cmdRetry(runId: string): Promise<void> {
  await withPool(async (pool) => {
    const run = await getRun(pool, runId);
    if (!run) {
      console.error(`Run not found: ${runId}`);
      process.exit(1);
    }

    const retried = await retryFailedRun(pool, runId);
    if (!retried) {
      console.error(`Run is not retryable: ${runId} has status ${run.status}`);
      process.exit(1);
    }

    await insertEvent(pool, runId, "run.retry.manual", {});
    console.log(`Retried run ${runId}.`);
  });
}

async function tableExists(pool: pg.Pool, tableName: string): Promise<boolean> {
  const result = await pool.query<{ exists: string | null }>(
    `SELECT to_regclass($1) AS exists`,
    [`public.${tableName}`],
  );
  return result.rows[0]?.exists !== null;
}

async function cmdDoctor(migrationsPath?: string): Promise<void> {
  const url = process.env["DATABASE_URL"];
  let ok = true;

  function check(name: string, passed: boolean, detail?: string): void {
    ok = ok && passed;
    const status = passed ? "ok" : "fail";
    console.log(`${status.padEnd(5)} ${name}${detail ? ` ${detail}` : ""}`);
  }

  if (!url) {
    check("DATABASE_URL", false, "missing");
    process.exit(1);
  }

  const pool = createPool(url);
  try {
    await pool.query(`SELECT 1`);
    check("database connection", true);

    const requiredTables = ["stela_migrations", "stela_runs", "stela_steps", "stela_events"];
    for (const table of requiredTables) {
      check(`table ${table}`, await tableExists(pool, table));
    }

    const migrationsDir = resolve(process.cwd(), migrationsPath ?? "migrations");
    let files: string[] = [];
    try {
      files = (await readdir(migrationsDir))
        .filter((file) => file.endsWith(".sql"))
        .sort();
      check("migrations directory", true, migrationsDir);
    } catch {
      check("migrations directory", false, migrationsDir);
    }

    if (await tableExists(pool, "stela_migrations")) {
      const applied = await loadAppliedMigrations(pool);
      const missing = files.filter((file) => !applied.has(file));
      check(
        "migration versions",
        missing.length === 0,
        missing.length === 0 ? `${applied.size} applied` : `missing ${missing.join(", ")}`,
      );
    }
  } catch (err) {
    check("database connection", false, err instanceof Error ? err.message : String(err));
  } finally {
    await pool.end();
  }

  if (!ok) process.exit(1);
}

async function cmdList(): Promise<void> {
  await withPool(async (pool) => {
    const runs = await listRuns(pool, 20);
    if (runs.length === 0) {
      console.log("No runs found.");
    } else {
      console.log(`${"ID".padEnd(38)} ${"WORKFLOW".padEnd(30)} ${"STATUS".padEnd(12)} CREATED`);
      console.log("-".repeat(110));
      for (const run of runs) {
        console.log(
          `${run.id.padEnd(38)} ${run.workflow_name.padEnd(30)} ${run.status.padEnd(12)} ${run.created_at.toISOString()}`,
        );
      }
    }
  });
}

function printUsage(): void {
  console.log(`
stela — durable workflow runtime

Usage:
  stela migrate                            Run all SQL migrations in ./migrations/
  stela migrate --path <dir>               Run SQL migrations from a custom directory
  stela doctor                             Check database and migration health
  stela worker                             Show how to start a worker (see README)
  stela enqueue <workflow-name> <json>     Enqueue a new run
  stela inspect <run-id>                   Print run details and steps
  stela events <run-id>                    Print run events
  stela tail [run-id]                      Follow run events
  stela cancel <run-id> [reason]           Cancel a pending, running, or sleeping run
  stela resume <run-id>                    Resume a pending or sleeping run now
  stela dead-letter <run-id> [reason]      Move a run to dead letter
  stela retry <run-id>                     Retry a failed or dead-lettered run
  stela list                               List recent runs

Environment:
  DATABASE_URL    Postgres connection string (required)
`.trim());
}

async function main(): Promise<void> {
  const [, , command, ...args] = process.argv;

  switch (command) {
    case "migrate":
      if (args.length > 2 || (args[0] && args[0] !== "--path")) {
        console.error("Usage: stela migrate [--path <dir>]");
        process.exit(1);
      }
      if (args[0] === "--path" && !args[1]) {
        console.error("Usage: stela migrate --path <dir>");
        process.exit(1);
      }
      await cmdMigrate(args[0] === "--path" ? args[1] : undefined);
      break;
    case "doctor":
      if (args.length > 2 || (args[0] && args[0] !== "--path")) {
        console.error("Usage: stela doctor [--path <dir>]");
        process.exit(1);
      }
      if (args[0] === "--path" && !args[1]) {
        console.error("Usage: stela doctor --path <dir>");
        process.exit(1);
      }
      await cmdDoctor(args[0] === "--path" ? args[1] : undefined);
      break;
    case "worker":
      await cmdWorker(args);
      break;
    case "enqueue":
      if (!args[0] || !args[1]) {
        console.error("Usage: stela enqueue <workflow-name> <json-input>");
        process.exit(1);
      }
      await cmdEnqueue(args[0], args[1]);
      break;
    case "inspect":
      if (!args[0]) {
        console.error("Usage: stela inspect <run-id>");
        process.exit(1);
      }
      await cmdInspect(args[0]);
      break;
    case "events":
      if (!args[0]) {
        console.error("Usage: stela events <run-id>");
        process.exit(1);
      }
      await cmdEvents(args[0]);
      break;
    case "tail":
      if (args.length > 1) {
        console.error("Usage: stela tail [run-id]");
        process.exit(1);
      }
      await cmdTail(args[0]);
      break;
    case "cancel":
      if (!args[0]) {
        console.error("Usage: stela cancel <run-id> [reason]");
        process.exit(1);
      }
      await cmdCancel(args[0], args.slice(1).join(" ") || undefined);
      break;
    case "resume":
      if (!args[0]) {
        console.error("Usage: stela resume <run-id>");
        process.exit(1);
      }
      await cmdResume(args[0]);
      break;
    case "dead-letter":
      if (!args[0]) {
        console.error("Usage: stela dead-letter <run-id> [reason]");
        process.exit(1);
      }
      await cmdDeadLetter(args[0], args.slice(1).join(" ") || undefined);
      break;
    case "retry":
      if (!args[0]) {
        console.error("Usage: stela retry <run-id>");
        process.exit(1);
      }
      await cmdRetry(args[0]);
      break;
    case "list":
      await cmdList();
      break;
    default:
      printUsage();
      if (command) process.exit(1);
      break;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
