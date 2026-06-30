import type { WorkflowContext, WorkflowDefinition, WorkflowOptions } from "./types.js";

/**
 * Define a durable workflow.
 *
 * @param name - Unique identifier for this workflow (used for persistence and routing).
 * @param fn   - Async function containing the workflow logic.
 * @param opts - Optional workflow execution settings.
 */
export function workflow<TInput = unknown, TOutput = void>(
  name: string,
  fn: (ctx: WorkflowContext<TInput>) => Promise<TOutput>,
  opts?: WorkflowOptions,
): WorkflowDefinition<TInput, TOutput> {
  return { name, fn, opts };
}
