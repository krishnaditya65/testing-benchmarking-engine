import {
  IamPermissionDriftDetector,
  InMemoryIamApiClientAdapter,
  type IamPolicyDocument,
} from '../drift/iam-permission-drift-detector';

const BASELINE_POLICY: IamPolicyDocument = {
  version: '2012-10-17',
  statements: [
    {
      sid: 'AllowTelemetryQueryReadOnly',
      effect: 'Allow',
      actions: ['s3:GetObject', 's3:ListBucket'],
      resources: ['arn:aws:s3:::testforge-telemetry-lake-cold-storage'],
    },
  ],
};

describe('IamPermissionDriftDetector', () => {
  it('reports no drift when the live policy document exactly matches the baseline', () => {
    const detector = new IamPermissionDriftDetector(new InMemoryIamApiClientAdapter());
    const report = detector.detect('telemetry-query-api-role', BASELINE_POLICY, BASELINE_POLICY);

    expect(report.hasDrift).toBe(false);
    expect(report.hasSecurityRelevantAddition).toBe(false);
    expect(report.drifts).toHaveLength(0);
  });

  it('flags an added Allow permission as HIGH severity and a security-relevant addition', () => {
    const detector = new IamPermissionDriftDetector(new InMemoryIamApiClientAdapter());
    const liveWithExtraPermission: IamPolicyDocument = {
      version: '2012-10-17',
      statements: [
        ...BASELINE_POLICY.statements,
        {
          sid: 'UnreviewedManualConsoleChange',
          effect: 'Allow',
          actions: ['s3:DeleteObject'],
          resources: ['arn:aws:s3:::testforge-telemetry-lake-cold-storage'],
        },
      ],
    };

    const report = detector.detect('telemetry-query-api-role', BASELINE_POLICY, liveWithExtraPermission);

    expect(report.hasDrift).toBe(true);
    expect(report.hasSecurityRelevantAddition).toBe(true);
    expect(report.drifts).toContainEqual({
      severity: 'HIGH',
      direction: 'ADDED',
      effect: 'Allow',
      action: 's3:DeleteObject',
      resource: 'arn:aws:s3:::testforge-telemetry-lake-cold-storage',
      description:
        'Role "telemetry-query-api-role" gained an UNREVIEWED Allow permission: s3:DeleteObject on arn:aws:s3:::testforge-telemetry-lake-cold-storage',
    });
  });

  it('flags a removed permission as INFORMATIONAL, not security-relevant', () => {
    const detector = new IamPermissionDriftDetector(new InMemoryIamApiClientAdapter());
    const liveWithFewerPermissions: IamPolicyDocument = {
      version: '2012-10-17',
      statements: [
        {
          sid: 'AllowTelemetryQueryReadOnly',
          effect: 'Allow',
          actions: ['s3:GetObject'],
          resources: ['arn:aws:s3:::testforge-telemetry-lake-cold-storage'],
        },
      ],
    };

    const report = detector.detect('telemetry-query-api-role', BASELINE_POLICY, liveWithFewerPermissions);

    expect(report.hasDrift).toBe(true);
    expect(report.hasSecurityRelevantAddition).toBe(false);
    expect(report.drifts).toContainEqual({
      severity: 'INFORMATIONAL',
      direction: 'REMOVED',
      effect: 'Allow',
      action: 's3:ListBucket',
      resource: 'arn:aws:s3:::testforge-telemetry-lake-cold-storage',
      description:
        'Role "telemetry-query-api-role" lost a previously-baselined Allow permission: s3:ListBucket on arn:aws:s3:::testforge-telemetry-lake-cold-storage',
    });
  });

  it('does not flag an added Deny statement as security-relevant (it tightens, not loosens, access)', () => {
    const detector = new IamPermissionDriftDetector(new InMemoryIamApiClientAdapter());
    const liveWithExtraDeny: IamPolicyDocument = {
      version: '2012-10-17',
      statements: [
        ...BASELINE_POLICY.statements,
        {
          sid: 'ExplicitDenyDeleteBucket',
          effect: 'Deny',
          actions: ['s3:DeleteBucket'],
          resources: ['arn:aws:s3:::testforge-telemetry-lake-cold-storage'],
        },
      ],
    };

    const report = detector.detect('telemetry-query-api-role', BASELINE_POLICY, liveWithExtraDeny);

    expect(report.hasDrift).toBe(true);
    expect(report.hasSecurityRelevantAddition).toBe(false);
    const denyDrift = report.drifts.find((d) => d.effect === 'Deny');
    expect(denyDrift?.severity).toBe('INFORMATIONAL');
  });

  it('resolves the live policy document through the configured IamApiClientPort', async () => {
    const adapter = new InMemoryIamApiClientAdapter();
    const liveWithExtraPermission: IamPolicyDocument = {
      version: '2012-10-17',
      statements: [
        ...BASELINE_POLICY.statements,
        { effect: 'Allow', actions: ['iam:PassRole'], resources: ['*'] },
      ],
    };
    adapter.seedLivePolicyDocument('telemetry-query-api-role', liveWithExtraPermission);
    const detector = new IamPermissionDriftDetector(adapter);

    const report = await detector.detectAgainstLiveRole('telemetry-query-api-role', BASELINE_POLICY);

    expect(report.hasSecurityRelevantAddition).toBe(true);
    expect(report.drifts.some((d) => d.action === 'iam:PassRole' && d.resource === '*')).toBe(true);
  });
});
