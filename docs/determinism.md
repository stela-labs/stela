# Deterministic Workflows

Stela replays workflow functions from the top whenever a run is claimed. Completed `step.run` calls return persisted results, so the step function is not called again after completion.

Keep workflow code deterministic:

- Put external side effects inside `step.run`.
- Give every step and sleep a stable name.
- Do not branch on `Date.now()`, random values, process state, or network calls outside a step.
- Do not rename completed steps unless you intentionally want a new persisted step.
- Use `sleep(name, duration)` for durable waiting instead of timers in workflow code.
- Keep input payloads JSON-serializable.

Useful pattern:

```ts
const wf = workflow("invoice.send", async ({ input, step, sleep }) => {
  const invoice = await step.run("create-invoice", () => createInvoice(input.customerId));
  await sleep("wait-before-reminder", "1d");
  await step.run("send-reminder", () => sendReminder(invoice.id));
});
```

Risky pattern:

```ts
const wf = workflow("invoice.send", async ({ input, step }) => {
  if (Date.now() % 2 === 0) {
    await step.run("send-a", () => sendA(input.customerId));
  } else {
    await step.run("send-b", () => sendB(input.customerId));
  }
});
```
