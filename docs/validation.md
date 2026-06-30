# Validation Patterns

Stela does not bind the runtime to one validation library. Validate before enqueueing, at workflow boundaries, or inside a named step depending on where you want failures to appear.

## Before Enqueue

```ts
interface OrderInput {
  orderId: string;
}

function parseOrderInput(value: unknown): OrderInput {
  if (
    typeof value === "object" &&
    value !== null &&
    "orderId" in value &&
    typeof value.orderId === "string"
  ) {
    return { orderId: value.orderId };
  }
  throw new Error("Invalid order input.");
}

const input = parseOrderInput(payload);
await client.start(orderWorkflow, input);
```

## Inside a Step

Use a step when validation itself should be durable and visible in run inspection.

```ts
const orderWorkflow = workflow<OrderInput>("order.fulfill", async ({ input, step }) => {
  const order = await step.run("validate-order", () => validateOrder(input.orderId));
  await step.run("charge-card", () => chargeCard(order.id));
});
```

This works with handwritten validators, Zod, Valibot, TypeBox, or application-specific validation code.
