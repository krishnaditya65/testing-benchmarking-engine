import { Field, InputType } from '@nestjs/graphql';
import { ArrayMinSize, IsArray, IsOptional, IsString, IsUUID, MaxLength, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { PipelineEdgeInput, PipelineNodeInput } from './pipeline-node.input';

@InputType()
export class CreatePipelineDefinitionInput {
  @Field()
  @IsString()
  @MaxLength(255)
  name: string;

  @Field({ nullable: true })
  @IsOptional()
  @IsString()
  description?: string;

  @Field()
  @IsUUID()
  ownerId: string;

  @Field(() => [PipelineNodeInput])
  @IsArray()
  @ArrayMinSize(1, { message: 'A pipeline definition must contain at least one DAG node' })
  @ValidateNested({ each: true })
  @Type(() => PipelineNodeInput)
  nodes: PipelineNodeInput[];

  @Field(() => [PipelineEdgeInput])
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => PipelineEdgeInput)
  edges: PipelineEdgeInput[];
}
