package org.neo4j.cypher.internal.runtime.zombie.aggregators

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Literal, Null}
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.expressions.{CountStar, FunctionInvocation, functions}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class AggregatorFactory(physicalPlan: PhysicalPlan, expressionConverters: ExpressionConverters) {

  /**
    * Creates a new [[Aggregator]] from an input AST Expression. Will also return the command [[Expression]]
    * required to compute the aggregator input value.
    */
  def newAggregator(expression: org.neo4j.cypher.internal.v4_0.expressions.Expression): (Aggregator, Expression) =
    expression match {
      case _: CountStar => (CountStarAggregator, Null())
      case unsupported =>
        throw new CantCompileQueryException(s"Aggregating function $unsupported is not yet supported in the parallel runtime")
//      case c: FunctionInvocation =>
//        c.function match {
//          case _: AggregatingFunction if c.distinct =>
//            throw new CantCompileQueryException("Distinct aggregating functions are not yet supported by the morsel runtime")
//
//          case functions.Count =>
//            CountOperatorExpression(self.toCommandExpression(id, c.arguments.head))
//
//          case functions.Sum =>
//            SumOperatorExpression(self.toCommandExpression(id, c.arguments.head))
//
//          case functions.Avg =>
//            AvgOperatorExpression(self.toCommandExpression(id, c.arguments.head))
//
//          case functions.Max =>
//            MaxOperatorExpression(self.toCommandExpression(id, c.arguments.head))
//
//          case functions.Min =>
//            MinOperatorExpression(self.toCommandExpression(id, c.arguments.head))
//
//          case functions.Collect =>
//            CollectOperatorExpression(self.toCommandExpression(id, c.arguments.head))
//
//          case _: AggregatingFunction =>
//            throw new CantCompileQueryException(s"Aggregating function ${c.name} is not yet supported by the morsel runtime")
//
//          case functions.Linenumber | functions.Filename =>
//            throw new CantCompileQueryException(s"Function ${c.name} is not yet supported by the morsel runtime")
//
//          case _ => None
//        }
    }
}
