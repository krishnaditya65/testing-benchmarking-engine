import { PipelineControlPlaneRole } from './roles.decorator';

/**
 * Shape of the authenticated principal attached to the request/GraphQL
 * context by JwtStrategy after successful token verification.
 */
export interface AuthenticatedUser {
  /** Subject claim ("sub") — stable user identifier from the identity provider. */
  userId: string;
  email: string;
  /** Roles claim used by RbacGuard for authorization decisions. */
  roles: PipelineControlPlaneRole[];
}
