import { SetMetadata } from '@nestjs/common';

/**
 * Metadata key read by RbacGuard to determine which roles are permitted
 * to invoke a given resolver/handler.
 */
export const REQUIRED_ROLES_METADATA_KEY = 'pipeline-api-gateway:required-roles';

/**
 * Roles recognised across the control plane. Kept local to this service —
 * in the full monorepo this would be sourced from a shared RBAC package.
 */
export type PipelineControlPlaneRole =
  | 'pipeline-admin'
  | 'pipeline-editor'
  | 'pipeline-viewer'
  | 'pipeline-run-operator';

/**
 * Decorator applied to resolver/controller methods to declare which roles
 * are allowed to execute them. Consumed by RbacGuard.
 *
 * @example
 * @RequiredRoles('pipeline-admin', 'pipeline-editor')
 * @Mutation(() => PipelineDefinition)
 * createPipelineDefinition(...) { ... }
 */
export const RequiredRoles = (...roles: PipelineControlPlaneRole[]) =>
  SetMetadata(REQUIRED_ROLES_METADATA_KEY, roles);
