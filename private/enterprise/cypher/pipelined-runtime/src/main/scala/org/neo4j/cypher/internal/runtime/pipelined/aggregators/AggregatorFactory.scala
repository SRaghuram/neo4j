/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.expressions.{CountStar, FunctionInvocation, Null, functions, Expression => AstExpression}
import org.neo4j.exceptions.{CantCompileQueryException, SyntaxException}

case class AggregatorFactory(physicalPlan: PhysicalPlan) {

  /**
    * Creates a new [[Aggregator]] from an input AST Expression. Will also return the command [[AstExpression]]
    * required to compute the aggregator input value.
    */
  def newAggregator(expression: AstExpression): (Aggregator, AstExpression) =
    expression match {
        // TODO move somewhere else
      case e if e.arguments.exists(_.containsAggregate) =>
        throw new SyntaxException("Can't use aggregate functions inside of aggregate functions.")
      case e if !e.isDeterministic =>
        throw new SyntaxException("Can't use non-deterministic (random) functions inside of aggregate functions.")

      case _: CountStar => (CountStarAggregator, Null.NULL)
      case c: FunctionInvocation =>
        c.function match {
          case functions.Count if c.distinct =>
            (CountDistinctAggregator, c.arguments.head)

          case _: AggregatingFunction if c.distinct =>
            throw new CantCompileQueryException("Morsel does not yet support Distinct aggregating functions, use another runtime.")

          case functions.Count =>
            (CountAggregator, c.arguments.head)

          case functions.Sum =>
            (SumAggregator, c.arguments.head)

          case functions.Avg =>
            (AvgAggregator, c.arguments.head)

          case functions.Max =>
            (MaxAggregator, c.arguments.head)

          case functions.Min =>
            (MinAggregator, c.arguments.head)

          case functions.Collect =>
            (CollectAggregator, c.arguments.head)

          case _: AggregatingFunction =>
            throw new CantCompileQueryException(s"Morsel does not yet support the Aggregating function `${c.name}`, use another runtime.")

          case _ =>
            throw new SyntaxException(s"Unexpected function in aggregating function position: ${c.name}")
        }
      case unsupported =>
        throw new SyntaxException(s"Unexpected expression in aggregating function position: $unsupported")
    }
}
