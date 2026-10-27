/**
 * index.ts
 *
 * Public entry point of `contract-healing-service`. Re-exports the diffing, healing, and
 * source-control-integration surface other services/scripts (e.g. a CI job wiring this
 * together) are expected to consume.
 */
export * from "./domain/api-schema-diff.types";
export * from "./diffing/openapi-schema-diff-detector";
export * from "./healing/jsonpath-extraction-auto-healer";
export * from "./healing/healing-proposal-pull-request-drafter";
