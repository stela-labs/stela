# Agent Guide

Stela is a local-first, self-hosted TypeScript workflow runtime backed by Postgres. It provides durable workflow runs, step caching, sleeps, retries, event logs, and a CLI for operating workers and runs.

## Repository Layout

- `packages/core`: public SDK, executor, worker, database layer, workflow helpers, and tests.
- `packages/cli`: `stela` CLI for migrations, workers, enqueueing, inspection, event tailing, cancellation, retry, and dead-letter operations.
- `migrations`: forward-only SQL migrations used by the CLI.
- `examples`: runnable workflows for basic, payment, and email-drip use cases.
- `docs`: operational and design guidance.
- `brand`: public brand assets.

## Commands

```bash
npm install
npm run build
npm test
```

For local Postgres:

```bash
docker compose up -d postgres
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
```

## Runtime Invariants

- Workflow functions are replayed from the top when a run resumes.
- `step.run` is the side-effect boundary; completed steps must return persisted results during replay.
- `sleep` must persist a wake time and halt execution until the run is claimable again.
- Retries and dead-letter state must be persisted in Postgres.
- Migrations must be forward-only and compatible with existing data.
- Avoid hidden global state in workflow execution paths.

## Change Guidelines

- Keep the runtime small and explicit.
- Add tests for executor, worker, retry, sleep, and migration behavior changes.
- Update README/docs for public API or CLI changes.
- Prefer clear TypeScript types over implicit JSON shapes.
- Do not introduce hosted-service assumptions into core runtime code.
