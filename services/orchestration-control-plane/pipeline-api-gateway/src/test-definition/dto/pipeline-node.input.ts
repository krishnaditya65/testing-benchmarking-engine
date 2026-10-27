import { Field, ID, InputType } from '@nestjs/graphql';
import { IsIn, IsNotEmpty, IsObject, IsString } from 'class-validator';
import { JsonScalar } from '../../common/json.scalar';
import { PipelineStepTypeEnum } from '../pipeline-node.object-type';

/** Input shape for a single DAG node when creating/updating a pipeline definition. */
@InputType('PipelineNodeInput')
export class PipelineNodeInput {
  @Field(() => ID)
  @IsString()
  @IsNotEmpty()
  id: string;

  @Field(() => PipelineStepTypeEnum)
  @IsIn(Object.values(PipelineStepTypeEnum))
  stepType: PipelineStepTypeEnum;

  @Field()
  @IsString()
  @IsNotEmpty()
  name: string;

  @Field(() => JsonScalar)
  @IsObject()
  config: Record<string, unknown>;
}

/** Input shape for a single DAG edge when creating/updating a pipeline definition. */
@InputType('PipelineEdgeInput')
export class PipelineEdgeInput {
  @Field(() => ID)
  @IsString()
  @IsNotEmpty()
  id: string;

  @Field()
  @IsString()
  @IsNotEmpty()
  fromNodeId: string;

  @Field()
  @IsString()
  @IsNotEmpty()
  toNodeId: string;
}
