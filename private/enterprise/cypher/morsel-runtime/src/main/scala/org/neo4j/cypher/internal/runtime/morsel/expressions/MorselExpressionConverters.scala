/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => commandexpressions}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{NestedPipeExpression, Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.expressions.{functions, _}
import org.neo4j.cypher.internal.v4_0.logical.plans.{NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

object MorselExpressionConverters extends ExpressionConverter {

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    case _: CountStar => Some(CountStarOperatorExpression)
    case c: FunctionInvocation =>
      c.function match {
        case _: AggregatingFunction if c.distinct =>
          throw new CantCompileQueryException("Distinct aggregating functions are not yet supported by the morsel runtime")

        case functions.Count =>
          Some(CountOperatorExpression(self.toCommandExpression(id, c.arguments.head)))

// Disable Avg since we currently do not support Duration avg
// and implement average different from interpreted (no cumulative moving avg)
//        case functions.Avg =>
//          Some(AvgOperatorExpression(self.toCommandExpression(id, c.arguments.head)))

        case functions.Max =>
          Some(MaxOperatorExpression(self.toCommandExpression(id, c.arguments.head)))

        case functions.Min =>
          Some(MinOperatorExpression(self.toCommandExpression(id, c.arguments.head)))

        case functions.Collect =>
          Some(CollectOperatorExpression(self.toCommandExpression(id, c.arguments.head)))

        case _: AggregatingFunction if c.distinct =>
          throw new CantCompileQueryException(s"Aggregating function ${c.name} is not yet supported by the morsel runtime")

        case _ => None
      }

    // We need to convert NestedPipeExpression here so we can register a noop dummy pipe as owner
    // TODO: Later we may want to establish a connection with the id of the operator for proper PROFILE support
    case e: NestedPipeExpression => {
      val ce = commandexpressions.NestedPipeExpression(e.pipe, self.toCommandExpression(id, e.projection))
      ce.registerOwningPipe(new NoPipe)
      Some(ce)
    }

    //Queries containing these expression cant be handled by morsel runtime yet
    case e: NestedPlanExpression => throw new CantCompileQueryException(s"$e is not yet supported by the morsel runtime")

    //UDFs may contain arbitrary CORE API reads which are not thread-safe. We only allow those that are explicitly marked as such
    case ResolvedFunctionInvocation(namespace, Some(signature), _) if signature.threadSafe || namespace.namespace.isEmpty => None
    case _:ResolvedFunctionInvocation => throw new CantCompileQueryException(s"User-defined functions is not yet supported by the morsel runtime")

    case _ => None
  }

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = None

  override def toGroupingExpression(id: Id,
                                    groupings: Map[String, ast.Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = None

  private class NoPipe() extends Pipe {
    override def id: Id = Id.INVALID_ID

    override def createResults(state: QueryState): Iterator[ExecutionContext] =
      throw new InternalException("Cannot create results on NoPipe")

    override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
      throw new InternalException("Cannot create results on NoPipe")

  }
}
