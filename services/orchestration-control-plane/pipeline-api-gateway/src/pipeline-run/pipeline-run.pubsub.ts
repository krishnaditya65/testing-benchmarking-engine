/**
 * DI token for the graphql-subscriptions PubSub instance used to fan out
 * live pipelineRunStatusUpdated events to GraphQL subscribers.
 *
 * NOTE (see SCALING.md): the provider registered under this token in
 * pipeline-run.module.ts is the in-memory `PubSub` from
 * `graphql-subscriptions`, which only fans out events within a single
 * process. Running more than one replica of this gateway requires swapping
 * it for a Redis-backed implementation (e.g. `graphql-redis-subscriptions`)
 * so a publish on one pod is seen by subscribers connected to another.
 */
export const PIPELINE_RUN_PUB_SUB = Symbol('PIPELINE_RUN_PUB_SUB');
