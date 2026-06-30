# Contributing

Stela is a Postgres-backed TypeScript workflow runtime. Contributions should keep the runtime small, deterministic, and operationally predictable.

## Development

```bash
npm install
npm run build
npm test
```

Use Node 20+ and Postgres 14+ for local testing. The bundled `docker-compose.yml` starts a local Postgres instance:

```bash
docker compose up -d postgres
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
```

## Pull Requests

- Keep changes narrowly scoped.
- Add or update tests for runtime behavior changes.
- Update docs when user-facing behavior, CLI flags, migrations, or public APIs change.
- Preserve deterministic replay semantics: completed steps must replay from stored results without re-running side effects.
- Use conventional commit messages for commits and PR titles, such as `fix: handle sleeping runs` or `docs: clarify migrations`.

## Runtime Invariants

- Workflow code may be replayed from the beginning.
- `step.run` callbacks are the boundary for side effects.
- Completed steps must return cached results during replay.
- Sleeps must persist wake times and resume without losing run state.
- Database migrations must be forward-only and safe to run once.
