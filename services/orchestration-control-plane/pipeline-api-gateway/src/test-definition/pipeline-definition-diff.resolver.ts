import { UseGuards } from '@nestjs/common';
import { Args, Field, ID, Int, ObjectType, Query, Resolver } from '@nestjs/graphql';
import { AuthGuard } from '@nestjs/passport';
import { DagStructuralChangeKind } from '@shared-libraries/dag-domain-model';
import { JsonScalar } from '../common/json.scalar';
import { RbacGuard } from '../auth/rbac.guard';
import { RequiredRoles } from '../auth/roles.decorator';
import { PipelineDefinitionDiffService } from './pipeline-definition-diff.service';

/**
 * GraphQL projections of `@shared-libraries/dag-domain-model`'s
 * `DagStructuralDiffReport` contract (see
 * `dag-structural-diff.types.ts`). Interfaces have no runtime
 * representation Nest/GraphQL can build SDL from, so — same pattern as
 * `pipeline-node.object-type.ts` mirrors `dag-domain-types.ts` — this file
 * re-declares the shared interfaces as `@ObjectType`-decorated classes.
 * `PipelineDefinitionDiffService.diffVersions` returns plain objects that
 * satisfy the shared interfaces structurally, which TypeScript accepts as
 * this resolver's return values without any explicit mapping step.
 */

@ObjectType('DagDiffNode')
class DagDiffNodeObjectType {
  @Field(() => ID)
  nodeId: string;

  @Field()
  kind: string;

  @Field(() => JsonScalar)
  config: Record<string, unknown>;

  @Field(() => [String])
  parentNodeIds: string[];
}

@ObjectType('DagDiffEdge')
class DagDiffEdgeObjectType {
  @Field(() => ID)
  fromNodeId: string;

  @Field(() => ID)
  toNodeId: string;

  @Field({ nullable: true })
  condition?: string;
}

@ObjectType('NodeStructuralChange')
class NodeStructuralChangeObjectType {
  @Field(() => String)
  kind: DagStructuralChangeKind;

  @Field(() => ID)
  nodeId: string;

  @Field(() => DagDiffNodeObjectType, { nullable: true })
  fromNode?: DagDiffNodeObjectType;

  @Field(() => DagDiffNodeObjectType, { nullable: true })
  toNode?: DagDiffNodeObjectType;

  @Field(() => [String])
  modifiedConfigFields: string[];

  @Field()
  summary: string;
}

@ObjectType('EdgeStructuralChange')
class EdgeStructuralChangeObjectType {
  @Field(() => String)
  kind: DagStructuralChangeKind;

  @Field(() => DagDiffEdgeObjectType)
  edge: DagDiffEdgeObjectType;

  @Field()
  summary: string;
}

@ObjectType('DagStructuralDiffReport')
class DagStructuralDiffReportObjectType {
  @Field(() => ID)
  pipelineId: string;

  @Field(() => Int)
  fromVersion: number;

  @Field(() => Int)
  toVersion: number;

  @Field()
  identicalContent: boolean;

  @Field(() => [NodeStructuralChangeObjectType])
  nodeChanges: NodeStructuralChangeObjectType[];

  @Field(() => [EdgeStructuralChangeObjectType])
  edgeChanges: EdgeStructuralChangeObjectType[];
}

/**
 * Exposes `pipelineDefinitionDiff(pipelineId, fromVersion, toVersion)` — the
 * GraphQL entry point for the "DAG definition versioning & structural
 * diffing" feature. Read-only, so it only requires viewer-level RBAC (any
 * role that can already read a `PipelineDefinition` can diff its history).
 */
@Resolver()
@UseGuards(AuthGuard('jwt'), RbacGuard)
export class PipelineDefinitionDiffResolver {
  constructor(private readonly diffService: PipelineDefinitionDiffService) {}

  @Query(() => DagStructuralDiffReportObjectType, { name: 'pipelineDefinitionDiff' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  pipelineDefinitionDiff(
    @Args('pipelineId', { type: () => ID }) pipelineId: string,
    @Args('fromVersion', { type: () => Int }) fromVersion: number,
    @Args('toVersion', { type: () => Int }) toVersion: number,
  ): Promise<DagStructuralDiffReportObjectType> {
    return this.diffService.diffVersions(pipelineId, fromVersion, toVersion);
  }
}
