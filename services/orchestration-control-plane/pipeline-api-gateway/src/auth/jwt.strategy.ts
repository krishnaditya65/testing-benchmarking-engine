import { Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { AuthenticatedUser } from './authenticated-user';
import { PipelineControlPlaneRole } from './roles.decorator';

/**
 * Raw JWT payload shape expected from the identity provider fronting
 * pipeline-api-gateway (e.g. an OIDC-compliant auth service). Only the
 * claims this gateway cares about are modelled here.
 */
interface PipelineControlPlaneJwtPayload {
  sub: string;
  email: string;
  roles?: string[];
  iss: string;
  exp: number;
}

/**
 * Verifies bearer JWTs issued to frontend/CLI clients of pipeline-api-gateway.
 * Signature verification and expiry checking are handled by passport-jwt
 * itself (via `secretOrKey` + `ignoreExpiration: false`); this strategy is
 * responsible for issuer pinning and shaping the payload into
 * AuthenticatedUser.
 */
@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  private readonly expectedIssuer: string;

  constructor(private readonly configService: ConfigService) {
    const secret = configService.get<string>('AUTH_JWT_SECRET');
    if (!secret) {
      throw new Error(
        'AUTH_JWT_SECRET must be configured for pipeline-api-gateway to verify inbound JWTs',
      );
    }

    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: secret,
    });

    this.expectedIssuer = configService.get<string>(
      'AUTH_JWT_ISSUER',
      'https://auth.distributed-testing.internal/',
    );
  }

  /**
   * Called by passport-jwt after signature + expiry verification succeeds.
   * Performs issuer pinning and maps the token payload to the
   * AuthenticatedUser used throughout the rest of the gateway.
   */
  validate(payload: PipelineControlPlaneJwtPayload): AuthenticatedUser {
    if (payload.iss !== this.expectedIssuer) {
      throw new UnauthorizedException(
        `Token issuer "${payload.iss}" does not match expected issuer "${this.expectedIssuer}"`,
      );
    }

    if (!payload.sub || !payload.email) {
      throw new UnauthorizedException('Token payload is missing required subject/email claims');
    }

    return {
      userId: payload.sub,
      email: payload.email,
      roles: (payload.roles ?? []) as PipelineControlPlaneRole[],
    };
  }
}
