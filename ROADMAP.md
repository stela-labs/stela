# Stela Roadmap

Stela is a local-first, self-hosted TypeScript durable workflow runtime. The roadmap favors a small, trustworthy core first: lightweight, fast, Postgres-backed, and easy for one technical team to operate without adopting a larger workflow platform.

## Product Principles

- Local-first and self-hosted by default.
- TypeScript-first API and documentation.
- Postgres as the durable persistence layer.
- Minimal moving parts: app code, worker, database, CLI.
- Free/OSS core must be useful for real workloads.
- Pro should sell production operations, not basic durability.
- Avoid artificial limits in self-hosted OSS usage.
- Prefer boring, inspectable runtime behavior over hidden control planes.

## Free / OSS Core

The free core should be the durable workflow runtime developers trust enough to run in their own applications.

### Current Core

- TypeScript workflow definition with `workflow(...)`.
- Durable step execution with `step.run(...)`.
- Cached step results on replay.
- Durable sleeps with `sleep(...)`.
- Postgres-backed run, step, and event persistence.
- Worker polling with row-level claiming.
- Worker leases stored in Postgres.
- Expired `running` run reclaim after worker crashes.
- Worker lease heartbeat for long-running executions.
- Lease-guarded run and step updates while a worker owns a run.
- Retries with exponential backoff.
- Per-step `maxAttempts`.
- Per-step timeout support.
- Workflow-level timeout support.
- Atomic retry scheduling for retry state transitions.
- Idempotency keys for duplicate prevention.
- Run cancellation.
- Manual resume for pending or sleeping runs.
- Dead-letter status for exhausted or operator-paused runs.
- Manual retry for failed or dead-lettered runs.
- Migration version tracking.
- CLI commands for migration, custom migration paths, doctor, enqueue, inspect, events, tail, cancel, resume, dead-letter, retry, list, and worker startup.
- Run event inspection.
- Step timing and retry details in inspect output.
- Structured JSON logging option.
- Configurable runtime log levels.
- Simple metrics hooks without requiring a metrics vendor.
- Event types for claim, heartbeat, cancel, retry, timeout, recovery, resume, and dead letter.
- Deterministic workflow guidance.
- Validation examples without binding the core to one schema library.
- Docker Compose for local development.
- Production deployment guide for one app worker plus Postgres.
- Basic example application.
- DB-backed test suite gated by `DATABASE_URL`.

### Free Roadmap

The original Free/OSS roadmap is now implemented in the core. Next Free/OSS work should be driven by review findings, production usage, and bug reports rather than expanding scope.

#### Free Polish Backlog

- Add more DB-backed stress tests for multiple workers.
- Add retention utilities for old events and completed runs.
- Add more examples for common workflow shapes.
- Improve package publishing metadata when the API stabilizes.

## Pro

Pro should target teams running Stela in production who need better operations, visibility, and policy controls. It should not lock away the basic durable execution engine.

### Pro Roadmap

#### Admin Dashboard

- Run list with filters for workflow, status, date, idempotency key, and worker.
- Run detail page with input, output, error, steps, attempts, sleeps, and events.
- Step timeline view.
- Manual retry, cancel, and resume actions.
- Failed-run triage view.
- Sleeping-run calendar/list view.
- Dead-letter queue view.

#### Queues and Concurrency

- Named queues.
- Per-queue concurrency limits.
- Per-workflow concurrency limits.
- Worker queue subscriptions.
- Priority queues.
- Backpressure visibility.

#### Scheduling

- Cron workflows.
- One-off scheduled workflows.
- Pause/resume schedules.
- Schedule history and next-run preview.
- Missed schedule handling policy.

#### Production Observability

- OpenTelemetry tracing.
- Metrics endpoint.
- Dashboard charts for throughput, latency, failures, retries, and queue depth.
- Alert destinations for failure spikes and stuck runs.
- Log retention controls.
- Event retention controls.
- Exportable run/event data.

#### Operational Safety

- Bulk retry and bulk cancel.
- Retry policies by workflow.
- Timeout policies by workflow.
- Maximum payload size policy.
- Retention policies by workflow.
- Read-only dashboard mode.
- Confirmation gates for destructive production actions.

#### Packaging

- Pro dashboard package.
- Docker image for dashboard.
- Production Docker Compose template.
- Helm chart once there is enough demand.
- Offline license file for self-hosted installs.

## Tier Boundary

Free/OSS should include everything required to run durable workflows safely:

- Durable execution.
- Persistence.
- Replays.
- Sleeps.
- Retries.
- Idempotency.
- Crash recovery.
- Cancellation.
- Core CLI operations.

Pro should include features that make production operation easier:

- Dashboard.
- Advanced queues.
- Concurrency controls.
- Schedules.
- Observability.
- Retention.
- Bulk operations.
- Operational policies.

## Near-Term Build Order

1. Run staff-level review of the completed Free/OSS core.
2. Fix review findings and add missing regression tests.
3. Start the Pro dashboard once the runtime state model is stable.
4. Add queues, concurrency, and schedules as Pro production operations.
