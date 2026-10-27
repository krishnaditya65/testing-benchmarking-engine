import { Field, ID, ObjectType, registerEnumType } from '@nestjs/graphql';
import { JsonScalar } from '../common/json.scalar';
import { PipelineStepType } from './dag-domain-types';

/** Enum of DAG node step types, mirrored from dag-domain-types.ts for GraphQL SDL. */
export enum PipelineStepTypeEnum {
  ApiRequest = 'ApiRequest',
  LoadTest = 'LoadTest',
  DataSeed = 'DataSeed',
  Assertion = 'Assertion',
  ChaosInjection = 'ChaosInjection',
  EnvironmentTeardown = 'EnvironmentTeardown',
}

registerEnumType(PipelineStepTypeEnum, { name: 'PipelineStepType' });

/** GraphQL projection of PipelineNode (see dag-domain-types.ts). */
@ObjectType('PipelineNode')
export class PipelineNodeObjectType {
  @Field(() => ID)
  id: string;

  @Field(() => PipelineStepTypeEnum)
  stepType: PipelineStepType;

  @Field()
  name: string;

  @Field(() => JsonScalar)
  config: Record<string, unknown>;
}

/** GraphQL projection of PipelineEdge (see dag-domain-types.ts). */
@ObjectType('PipelineEdge')
export class PipelineEdgeObjectType {
  @Field(() => ID)
  id: string;

  @Field()
  fromNodeId: string;

  @Field()
  toNodeId: string;
}
