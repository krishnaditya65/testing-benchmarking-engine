import { CanActivate, ExecutionContext, ForbiddenException, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { GqlExecutionContext } from '@nestjs/graphql';
import { AuthenticatedUser } from './authenticated-user';
import { PipelineControlPlaneRole, REQUIRED_ROLES_METADATA_KEY } from './roles.decorator';

/**
 * Enforces role-based access control on resolvers/handlers annotated with
 * @RequiredRoles(...). Runs after Passport's AuthGuard('jwt') has already
 * populated `request.user`, so it is typically composed as:
 *
 *   @UseGuards(AuthGuard('jwt'), RbacGuard)
 *   @RequiredRoles('pipeline-admin')
 *
 * Supports both plain HTTP handlers and GraphQL resolvers by resolving the
 * underlying request through GqlExecutionContext when applicable.
 */
@Injectable()
export class RbacGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const requiredRoles = this.reflector.getAllAndOverride<PipelineControlPlaneRole[]>(
      REQUIRED_ROLES_METADATA_KEY,
      [context.getHandler(), context.getClass()],
    );

    // No roles declared on this handler => authentication alone is sufficient.
    if (!requiredRoles || requiredRoles.length === 0) {
      return true;
    }

    const user = this.extractAuthenticatedUser(context);
    if (!user) {
      throw new ForbiddenException('No authenticated user present on request context');
    }

    const hasRequiredRole = requiredRoles.some((role) => user.roles.includes(role));
    if (!hasRequiredRole) {
      throw new ForbiddenException(
        `User ${user.userId} lacks required role(s): ${requiredRoles.join(', ')}`,
      );
    }

    return true;
  }

  private extractAuthenticatedUser(context: ExecutionContext): AuthenticatedUser | undefined {
    if (context.getType<'graphql'>() === 'graphql') {
      const gqlContext = GqlExecutionContext.create(context);
      return gqlContext.getContext().req?.user;
    }
    const httpRequest = context.switchToHttp().getRequest();
    return httpRequest?.user;
  }
}
