/**
 * healing-proposal-pull-request-drafter.ts
 *
 * A `HealingProposal` (see `jsonpath-extraction-auto-healer.ts`) never gets applied to a
 * `PipelineDefinition` silently by default — it becomes a pull request against the repo that
 * owns the pipeline's source-of-truth YAML/JSON so a human reviews the rewrite before it lands,
 * mirroring how `pipeline-api-gateway` treats `dag-workflow-scheduler` as a hexagonal port
 * rather than a hardcoded HTTP call (see that service's `dag-workflow-scheduler-client.port.ts`
 * for the sibling pattern this file follows).
 *
 * The ONE exception: proposals at or above `AUTO_APPLY_CONFIDENCE_THRESHOLD` (see
 * `jsonpath-extraction-auto-healer.ts`) may be auto-applied — i.e. the PR is opened
 * pre-approved/auto-mergeable rather than left for review — because at that confidence the
 * name+type similarity match is unambiguous enough (e.g. "customerEmail" -> "customerEmailAddress"
 * with an identical type) that the review overhead outweighs the risk. This threshold decision
 * is made by the *caller* of `SourceControlPort` (typically a small orchestration script wiring
 * the diff detector -> healer -> drafter together), not inside the adapter — the adapter's job
 * is only to open the PR with the correct auto-merge flag set, never to decide the threshold
 * itself, so the policy lives in exactly one place.
 */
import type { HealingProposal } from "./jsonpath-extraction-auto-healer";

export interface HealingPullRequestDraft {
  /** e.g. "checkout-load-test", matching a pipeline definition's `name` in
   * shared-libraries/dag-domain-model's PipelineDefinition. */
  pipelineName: string;
  pipelineId: string;
  proposals: HealingProposal[];
  /** True when every proposal in this draft is at/above the auto-apply confidence threshold —
   * see module doc comment. Decided by the caller, just carried here for the adapter to act on. */
  autoMergeEligible: boolean;
}

export interface DraftedPullRequestResult {
  pullRequestUrl: string;
  pullRequestNumber: number;
  /** Mirrors `HealingPullRequestDraft.autoMergeEligible` back so callers/tests can assert the
   * adapter respected the flag rather than re-deciding it. */
  wasAutoMergeRequested: boolean;
}

/**
 * SourceControlPort
 *
 * Hexagonal port: `contract-healing-service`'s domain/healing logic depends only on this
 * interface, never on a concrete Git host SDK, so swapping GitHub for GitLab/Bitbucket later is
 * an adapter change, not a domain change (same shape as `SecretProviderPort` in
 * `pipeline-api-gateway`).
 */
export interface SourceControlPort {
  draftHealingPullRequest(draft: HealingPullRequestDraft): Promise<DraftedPullRequestResult>;
}

export interface GitHubHealingAdapterConfig {
  ownerAndRepo: string; // e.g. "acme-corp/testforge-pipelines"
  baseBranch: string; // e.g. "main"
  /** Personal access token / GitHub App installation token. NOTE: in a real deployment this
   * comes from the same secret-injection mechanism as `pipeline-api-gateway`'s
   * `SecretProviderPort` (Vault/AWS Secrets Manager), not a plain env var — left as a
   * constructor parameter here since this service doesn't yet own a secret-injection module of
   * its own. */
  authToken: string;
}

/**
 * GitHubHealingPullRequestAdapter
 *
 * Stub GitHub adapter: shaped like a real `octokit`-based implementation (branch-per-proposal,
 * PR body summarizing every `HealingProposal`'s rationale) but stops short of making network
 * calls, since this service has no live GitHub credentials in this environment. Swap the body
 * of `draftHealingPullRequest` for real `octokit.rest.git.createRef` /
 * `octokit.rest.pulls.create` calls to make this live; the method signature and PR-body
 * construction below are what a real implementation would send.
 */
export class GitHubHealingPullRequestAdapter implements SourceControlPort {
  constructor(private readonly config: GitHubHealingAdapterConfig) {}

  async draftHealingPullRequest(draft: HealingPullRequestDraft): Promise<DraftedPullRequestResult> {
    const branchName = `contract-healing/${draft.pipelineId}-${Date.now()}`;
    const prBody = buildPullRequestBody(draft);

    // Real implementation would do roughly:
    //   const baseRef = await octokit.rest.git.getRef({ owner, repo, ref: `heads/${this.config.baseBranch}` });
    //   await octokit.rest.git.createRef({ owner, repo, ref: `refs/heads/${branchName}`, sha: baseRef.data.object.sha });
    //   await octokit.rest.repos.createOrUpdateFileContents({ owner, repo, branch: branchName, path: pipelineDefinitionPath, message: ..., content: base64(rewrittenYaml) });
    //   const pr = await octokit.rest.pulls.create({ owner, repo, head: branchName, base: this.config.baseBranch, title: ..., body: prBody });
    //   if (draft.autoMergeEligible) await octokit.rest.pulls.merge({ owner, repo, pull_number: pr.data.number, merge_method: "squash" });
    //
    // Stubbed here: no network I/O against `this.config.ownerAndRepo` happens, we just return a
    // deterministic, realistically-shaped result so callers/tests can exercise the port without
    // live GitHub credentials.
    return {
      pullRequestUrl: `https://github.com/${this.config.ownerAndRepo}/pull/stub-${branchName}`,
      pullRequestNumber: 0,
      wasAutoMergeRequested: draft.autoMergeEligible,
    };
  }
}

function buildPullRequestBody(draft: HealingPullRequestDraft): string {
  const lines = [
    `Autonomous Contract Healing detected ${draft.proposals.length} backend schema change(s) affecting pipeline "${draft.pipelineName}".`,
    "",
    ...draft.proposals.map(
      (proposal, index) =>
        `${index + 1}. \`${proposal.originalExpression}\` -> \`${proposal.proposedExpression}\` ` +
        `(confidence ${Math.round(proposal.confidenceScore * 100)}%)\n   ${proposal.rationale}`,
    ),
    "",
    draft.autoMergeEligible
      ? "All proposals are at/above the auto-apply confidence threshold; this PR is opened auto-merge-eligible."
      : "One or more proposals are below the auto-apply confidence threshold; please review before merging.",
  ];
  return lines.join("\n");
}
