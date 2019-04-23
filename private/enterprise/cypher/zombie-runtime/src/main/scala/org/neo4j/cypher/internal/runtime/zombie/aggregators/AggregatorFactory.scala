package org.neo4j.cypher.internal.runtime.zombie.aggregators

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.expressions.{CountStar, FunctionInvocation, Null, functions, Expression => AstExpression}

case class AggregatorFactory(physicalPlan: PhysicalPlan) {

  /**
    * Creates a new [[Aggregator]] from an input AST Expression. Will also return the command [[AstExpression]]
    * required to compute the aggregator input value.
    */
  def newAggregator(expression: AstExpression): (Aggregator, AstExpression) =
    expression match {
      case _: CountStar => (CountStarAggregator, Null.NULL)
      case c: FunctionInvocation =>
        c.function match {
          case _: AggregatingFunction if c.distinct =>
            throw new CantCompileQueryException("Distinct aggregating functions are not yet supported by the parallel runtime")

          case functions.Count =>
            (CountAggregator, c.arguments.head)

          case functions.Sum =>
            (SumAggregator, c.arguments.head)

          case functions.Avg =>
            (AvgAggregator, c.arguments.head)
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
          case _: AggregatingFunction =>
            throw new CantCompileQueryException(s"Aggregating function ${c.name} is not yet supported by the parallel runtime")

          case _ =>
            throw new CantCompileQueryException(s"Unexpected function in aggregating function position: ${c.name}")
        }
      case unsupported =>
        throw new CantCompileQueryException(s"Unexpected expression in aggregating function position: $unsupported")
    }
}
