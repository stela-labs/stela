# Examples

Stela examples are intentionally small and practical. They use mocked external systems so they run locally, but each step maps to a real integration boundary such as an LLM call, payment processor, helpdesk, CRM, document store, or incident tool.

## Core Examples

- `examples/basic`: order validation, payment, sleep, and confirmation email.
- `examples/payment`: idempotent payment processing with retries and durable result chaining.
- `examples/email-drip`: onboarding sequence with durable sleeps.

## Agentic Examples

- `examples/agent-research`: research agent with source collection, draft generation, policy review, human approval, and publish.
- `examples/support-triage`: support agent that classifies a ticket, fetches account context, drafts a reply, waits for review, and sends.
- `examples/incident-runbook`: operations agent that investigates telemetry, proposes remediation, waits for operator approval, and executes.

## Financial Services Examples

- `examples/financial-kyc`: KYC document request, upload signal, screening checks, review packet, and analyst decision.
- `examples/chargeback-evidence`: payment, fulfillment, and support evidence collection with human-approved submission.

## Pattern

Use `step.run` around every side effect. This includes LLM calls, retrieval, outbound messages, payment processor calls, ticket updates, document checks, and remediation commands.

Use `step.waitForSignal` for human or external decisions:

- `approval.received`
- `agent.reviewed`
- `document.uploaded`
- `analyst.decision`
- `packet.approved`
- `operator.approval`

Use `sleep` for durable waits such as reminders, dunning windows, submission cooldowns, and delayed follow-ups.

## Running An Example

```bash
docker compose up -d postgres
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npx stela migrate
npm run build
DATABASE_URL=postgres://stela:stela@localhost:55432/stela npm start -w examples/agent-research
```

Swap the workspace name for any example package.
