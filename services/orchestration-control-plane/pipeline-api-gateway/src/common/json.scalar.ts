import { Scalar, CustomScalar } from '@nestjs/graphql';
import { GraphQLScalarType, Kind, ValueNode } from 'graphql';

/**
 * Generic JSON scalar used wherever this gateway exposes free-form,
 * step-type-specific data over GraphQL (pipeline node configs, node status
 * maps, telemetry summaries). Avoids pulling in graphql-type-json as an
 * extra dependency for a handful of usages.
 */
@Scalar('JSON', () => GraphQLScalarType)
export class JsonScalar implements CustomScalar<unknown, unknown> {
  description = 'Arbitrary JSON value (object, array, string, number, boolean, or null)';

  parseValue(value: unknown): unknown {
    return value;
  }

  serialize(value: unknown): unknown {
    return value;
  }

  parseLiteral(ast: ValueNode): unknown {
    return this.parseAstNode(ast);
  }

  private parseAstNode(ast: ValueNode): unknown {
    switch (ast.kind) {
      case Kind.STRING:
      case Kind.BOOLEAN:
        return ast.value;
      case Kind.INT:
      case Kind.FLOAT:
        return Number(ast.value);
      case Kind.OBJECT:
        return ast.fields.reduce<Record<string, unknown>>((acc, field) => {
          acc[field.name.value] = this.parseAstNode(field.value);
          return acc;
        }, {});
      case Kind.LIST:
        return ast.values.map((value) => this.parseAstNode(value));
      case Kind.NULL:
        return null;
      default:
        return null;
    }
  }
}
