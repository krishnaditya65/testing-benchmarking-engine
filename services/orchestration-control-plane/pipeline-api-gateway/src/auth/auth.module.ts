import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PassportModule } from '@nestjs/passport';
import { APP_GUARD } from '@nestjs/core';
import { AuthGuard } from '@nestjs/passport';
import { JwtStrategy } from './jwt.strategy';
import { RbacGuard } from './rbac.guard';

/**
 * Wires Passport's JWT strategy and exposes RbacGuard for use across
 * resolvers/controllers. JwtStrategy and RbacGuard are exported so other
 * feature modules can compose `@UseGuards(AuthGuard('jwt'), RbacGuard)`.
 */
@Module({
  imports: [ConfigModule, PassportModule.register({ defaultStrategy: 'jwt' })],
  providers: [JwtStrategy, RbacGuard],
  exports: [PassportModule, JwtStrategy, RbacGuard],
})
export class AuthModule {}

// Re-exported for convenience so consumers of AuthModule don't need to
// import @nestjs/passport directly just to compose the JWT guard.
export { AuthGuard };
