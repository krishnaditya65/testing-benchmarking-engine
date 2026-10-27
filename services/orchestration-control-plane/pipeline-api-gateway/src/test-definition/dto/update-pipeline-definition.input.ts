import { Field, ID, InputType } from '@nestjs/graphql';
import { ArrayMinSize, IsArray, IsOptional, IsString, MaxLength, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { PipelineEdgeInput, PipelineNodeInput } from './pipeline-node.input';

/**
 * Partial-update input: only fields the caller supplies are patched onto
 * the existing PipelineDefinitionEntity. `nodes`/`edges`, when supplied,
 * fully replace the existing DAG (no partial-node merge semantics) and are
 * re-validated by TestDefinitionService against the same DAG-shape rules
 * used on create.
 */
@InputType()
export class UpdatePipelineDefinitionInput {
  @Field(() => ID)
  @IsString()
  id: string;

  @Field({ nullable: true })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  name?: string;

  @Field({ nullable: true })
  @IsOptional()
  @IsString()
  description?: string;

  @Field(() => [PipelineNodeInput], { nullable: true })
  @IsOptional()
  @IsArray()
  @ArrayMinSize(1, { message: 'A pipeline definition must contain at least one DAG node' })
  @ValidateNested({ each: true })
  @Type(() => PipelineNodeInput)
  nodes?: PipelineNodeInput[];

  @Field(() => [PipelineEdgeInput], { nullable: true })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => PipelineEdgeInput)
  edges?: PipelineEdgeInput[];
}
