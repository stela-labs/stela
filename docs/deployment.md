# Production Deployment

The smallest production shape is one application process, one or more worker processes, and Postgres.

## Runtime

- Run `stela migrate` before starting workers.
- Give each worker the same workflow registry.
- Start with one worker process and scale horizontally after checking lease and retry behavior.
- Keep `leaseDurationMs` comfortably larger than `heartbeatIntervalMs`.
- Use per-step `timeoutMs` for external calls.
- Use workflow-level `timeoutMs` for whole-run protection.

## Database

- Use managed Postgres or a backed-up self-hosted Postgres.
- Monitor connection count, disk usage, and replication/backups.
- Keep `stela_events` long enough for debugging, then prune through your own retention job until built-in retention exists.

## Operations

```bash
DATABASE_URL=postgres://user:pass@host:5432/stela stela migrate
DATABASE_URL=postgres://user:pass@host:5432/stela stela doctor
DATABASE_URL=postgres://user:pass@host:5432/stela node dist/worker.js
```

Use:

- `stela inspect <run-id>` for run state.
- `stela events <run-id>` for event history.
- `stela tail [run-id]` for live event following.
- `stela retry <run-id>` for failed or dead-lettered runs.
- `stela dead-letter <run-id>` to stop active processing for a run.
- `stela resume <run-id>` to wake pending or sleeping runs immediately.

## Logging And Metrics

Use `jsonLogs: true` for structured worker logs and pass `metrics` hooks to integrate with your existing metrics stack.

```ts
const stats = createStatsClient();

startWorker({
  connectionString,
  workflows,
  jsonLogs: true,
  logLevel: "info",
  metrics: {
    increment(name, tags) {
      stats.increment(name, tags);
    },
    timing(name, valueMs, tags) {
      stats.timing(name, valueMs, tags);
    },
  },
});
```
