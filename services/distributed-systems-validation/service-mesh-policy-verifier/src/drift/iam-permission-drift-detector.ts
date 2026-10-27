/**
 * Implements "Infrastructure Drift Detection" (feature-manifest section 5, "Distributed Systems
 * & Infrastructure Validation") for IAM: compares a live-fetched IAM policy document against an
 * expected baseline policy document (both AWS-style `{ Version, Statement[] }` JSON, the same
 * shape `infrastructure/terraform`'s IAM resources render to) and flags drift.
 *
 * Unlike `kubernetes-state-baseline-comparator.ts`, this detector treats the two drift
 * directions asymmetrically on purpose: a **removed** permission is a functionality regression
 * (something will now get an AccessDenied that used to work) but is not a security risk by
 * itself. An **added** permission is the security-relevant direction — it means the live
 * principal can now do something nobody reviewed and approved, which is exactly the class of
 * drift (a manually-clicked IAM console change, a misconfigured Terraform apply from a stale
 * branch) this exists to catch. Added-permission findings are therefore always reported at
 * `HIGH` severity; removed-permission findings at `INFORMATIONAL`.
 */

export type IamEffect = 'Allow' | 'Deny';

export interface IamPolicyStatement {
  readonly sid?: string;
  readonly effect: IamEffect;
  readonly actions: readonly string[];
  readonly resources: readonly string[];
}

export interface IamPolicyDocument {
  readonly version: string;
  readonly statements: readonly IamPolicyStatement[];
}

export type IamPermissionDriftSeverity = 'HIGH' | 'INFORMATIONAL';

export interface IamPermissionDrift {
  readonly severity: IamPermissionDriftSeverity;
  readonly direction: 'ADDED' | 'REMOVED';
  readonly effect: IamEffect;
  readonly resource: string;
  readonly action: string;
  readonly description: string;
}

export interface IamDriftReport {
  readonly roleIdentifier: string;
  readonly drifts: readonly IamPermissionDrift[];
  readonly hasDrift: boolean;
  /** True if any drift is an ADDED Allow permission — the specific condition a POLICY_VERIFYING gate should fail on. */
  readonly hasSecurityRelevantAddition: boolean;
}

/**
 * Port over the live IAM backend for a role/policy's currently-attached policy document.
 *
 * NOTE: real wiring requires an AWS SDK v3 client (`@aws-sdk/client-iam`, already a dependency
 * of `pipeline-api-gateway` for Secrets Manager access, so this service would add the sibling
 * IAM client rather than a new SDK):
 *
 *   const iam = new IAMClient({});
 *   const { PolicyVersion } = await iam.send(new GetPolicyVersionCommand({ PolicyArn, VersionId }));
 *   const documentJson = decodeURIComponent(PolicyVersion.Document);
 *   // parse documentJson into IamPolicyDocument shape (AWS's raw Statement[].Action/.Resource
 *   // fields are string | string[] and need normalizing to always-array before comparison).
 */
export interface IamApiClientPort {
  fetchLivePolicyDocument(roleIdentifier: string): Promise<IamPolicyDocument>;
}

/** Stub `IamApiClientPort` for local development and tests. */
export class InMemoryIamApiClientAdapter implements IamApiClientPort {
  private readonly documentsByRoleIdentifier = new Map<string, IamPolicyDocument>();

  seedLivePolicyDocument(roleIdentifier: string, document: IamPolicyDocument): void {
    this.documentsByRoleIdentifier.set(roleIdentifier, document);
  }

  async fetchLivePolicyDocument(roleIdentifier: string): Promise<IamPolicyDocument> {
    const document = this.documentsByRoleIdentifier.get(roleIdentifier);
    if (!document) {
      throw new Error(
        `InMemoryIamApiClientAdapter: no live policy document seeded for role "${roleIdentifier}" — ` +
          `real wiring fetches this via @aws-sdk/client-iam (see this class's doc comment)`,
      );
    }
    return document;
  }
}

/**
 * A single (effect, action, resource) grant, the atomic unit this detector diffs — expanding
 * each statement's actions × resources so a statement-level diff (which would miss "one action
 * added to an otherwise-unchanged statement") is never used in place of this finer-grained one.
 */
interface ExpandedGrant {
  readonly effect: IamEffect;
  readonly action: string;
  readonly resource: string;
}

function expandStatementsToGrants(statements: readonly IamPolicyStatement[]): ExpandedGrant[] {
  const grants: ExpandedGrant[] = [];
  for (const statement of statements) {
    for (const action of statement.actions) {
      for (const resource of statement.resources) {
        grants.push({ effect: statement.effect, action, resource });
      }
    }
  }
  return grants;
}

function grantKey(grant: ExpandedGrant): string {
  return `${grant.effect}|${grant.action}|${grant.resource}`;
}

/**
 * Compares a baseline IAM policy document against a live-fetched one and reports every added or
 * removed (effect, action, resource) grant, with added `Allow` grants flagged `HIGH` severity.
 */
export class IamPermissionDriftDetector {
  constructor(private readonly iamApiClient: IamApiClientPort) {}

  async detectAgainstLiveRole(roleIdentifier: string, baseline: IamPolicyDocument): Promise<IamDriftReport> {
    const liveDocument = await this.iamApiClient.fetchLivePolicyDocument(roleIdentifier);
    return this.detect(roleIdentifier, baseline, liveDocument);
  }

  /** Pure comparison, exercised directly by unit tests without any AWS API calls. */
  detect(roleIdentifier: string, baseline: IamPolicyDocument, live: IamPolicyDocument): IamDriftReport {
    const baselineGrants = expandStatementsToGrants(baseline.statements);
    const liveGrants = expandStatementsToGrants(live.statements);

    const baselineByKey = new Map(baselineGrants.map((g) => [grantKey(g), g]));
    const liveByKey = new Map(liveGrants.map((g) => [grantKey(g), g]));

    const drifts: IamPermissionDrift[] = [];

    for (const [key, grant] of liveByKey) {
      if (!baselineByKey.has(key)) {
        drifts.push({
          severity: grant.effect === 'Allow' ? 'HIGH' : 'INFORMATIONAL',
          direction: 'ADDED',
          effect: grant.effect,
          action: grant.action,
          resource: grant.resource,
          description:
            grant.effect === 'Allow'
              ? `Role "${roleIdentifier}" gained an UNREVIEWED Allow permission: ${grant.action} on ${grant.resource}`
              : `Role "${roleIdentifier}" gained a new Deny statement: ${grant.action} on ${grant.resource}`,
        });
      }
    }

    for (const [key, grant] of baselineByKey) {
      if (!liveByKey.has(key)) {
        drifts.push({
          severity: 'INFORMATIONAL',
          direction: 'REMOVED',
          effect: grant.effect,
          action: grant.action,
          resource: grant.resource,
          description: `Role "${roleIdentifier}" lost a previously-baselined ${grant.effect} permission: ${grant.action} on ${grant.resource}`,
        });
      }
    }

    return {
      roleIdentifier,
      drifts,
      hasDrift: drifts.length > 0,
      hasSecurityRelevantAddition: drifts.some((d) => d.direction === 'ADDED' && d.severity === 'HIGH'),
    };
  }
}
