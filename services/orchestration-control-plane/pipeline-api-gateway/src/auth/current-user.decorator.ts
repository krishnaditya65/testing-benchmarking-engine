import { ExecutionContext, createParamDecorator } from '@nestjs/common';
import { GqlExecutionContext } from '@nestjs/graphql';
import { AuthenticatedUser } from './authenticated-user';

/**
 * Param decorator that pulls the AuthenticatedUser populated by JwtStrategy
 * out of either an HTTP request or a GraphQL resolver context, e.g.:
 *
 *   createPipelineDefinition(@CurrentUser() user: AuthenticatedUser, ...)
 */
export const CurrentUser = createParamDecorator(
  (_data: unknown, context: ExecutionContext): AuthenticatedUser | undefined => {
    if (context.getType<'graphql'>() === 'graphql') {
      const gqlContext = GqlExecutionContext.create(context);
      return gqlContext.getContext().req?.user;
    }
    const httpRequest = context.switchToHttp().getRequest();
    return httpRequest?.user;
  },
);
