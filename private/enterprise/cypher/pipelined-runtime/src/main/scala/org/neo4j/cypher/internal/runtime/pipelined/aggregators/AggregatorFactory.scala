/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.ast.CollectAll
import org.neo4j.cypher.internal.physicalplanning.ast.IsEmpty
import org.neo4j.cypher.internal.physicalplanning.ast.NonEmpty
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.SyntaxException

case class AggregatorFactory(physicalPlan: PhysicalPlan) {

  /**
   * Creates a new [[Aggregator]] from an input AST Expression. Will also return the command [[expressions.Expression]]
   * required to compute the aggregator input value.
   */
  def newAggregator(expression: expressions.Expression): (Aggregator, Array[expressions.Expression]) =
    expression match {
      // TODO move somewhere else
      case e if e.arguments.exists(_.containsAggregate) =>
        throw new SyntaxException("Can't use aggregate functions inside of aggregate functions.")
      case e if !e.isDeterministic =>
        throw new SyntaxException("Can't use non-deterministic (random) functions inside of aggregate functions.")

      case _: CountStar => (CountStarAggregator, Array.empty)
      case CollectAll(expr) => (CollectAllAggregator, Array(expr))
      case NonEmpty() => (NonEmptyAggregator, Array.empty)
      case IsEmpty() => (IsEmptyAggregator, Array.empty)

      case c: FunctionInvocation =>
        c.function match {
          case functions.Count if c.distinct =>
            (CountDistinctAggregator, Array(c.arguments.head))

          case functions.Count =>
            (CountAggregator, Array(c.arguments.head))

          case functions.Sum if c.distinct =>
            (SumDistinctAggregator, Array(c.arguments.head))

          case functions.Sum =>
            (SumAggregator, Array(c.arguments.head))

          case functions.Max => // no difference if distinct
            (MaxAggregator, Array(c.arguments.head))

          case functions.Min => // no difference if distinct
            (MinAggregator, Array(c.arguments.head))

          case functions.Collect if c.distinct =>
            (CollectDistinctAggregator, Array(c.arguments.head))

          case functions.Collect =>
            (CollectAggregator, Array(c.arguments.head))

          case functions.Avg if c.distinct  =>
            (AvgDistinctAggregator, Array(c.arguments.head))

          case functions.Avg =>
            (AvgAggregator, Array(c.arguments.head))

          case functions.StdDev if c.distinct  =>
            (StdevDistinctAggregator, Array(c.arguments.head))

          case functions.StdDev =>
            (StdevAggregator, Array(c.arguments.head))

          case functions.StdDevP if c.distinct  =>
            (StdevPDistinctAggregator, Array(c.arguments.head))

          case functions.StdDevP =>
            (StdevPAggregator, Array(c.arguments.head))

          case _: AggregatingFunction =>
            throw new CantCompileQueryException(s"Pipelined does not yet support the Aggregating function `${c.name}`, use another runtime.")

          case _ =>
            throw new SyntaxException(s"Unexpected function in aggregating function position: ${c.name}")
        }
      case unsupported =>
        throw new SyntaxException(s"Unexpected expression in aggregating function position: $unsupported")
    }
}
