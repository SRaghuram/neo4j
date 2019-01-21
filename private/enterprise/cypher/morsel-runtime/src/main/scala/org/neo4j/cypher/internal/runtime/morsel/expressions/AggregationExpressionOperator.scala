/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.cypher.internal.v4_0.util.SyntaxException
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType

abstract class AggregationExpressionOperator extends Expression {

  def apply(ctx: ExecutionContext, state: OldQueryState) =
    throw new UnsupportedOperationException("Aggregations should not be used like this.")

  def createAggregationMapper: AggregationMapper
  def createAggregationReducer: AggregationReducer
}

abstract class AggregationExpressionOperatorWithInnerExpression(inner:Expression) extends AggregationExpressionOperator {
  if(inner.containsAggregate)
    throw new SyntaxException("Can't use aggregate functions inside of aggregate functions.")

  if(! inner.isDeterministic)
    throw new SyntaxException("Can't use non-deterministic (random) functions inside of aggregate functions.")

  def expectedInnerType: CypherType

  def arguments = Seq(inner)

  def symbolTableDependencies: Set[String] = inner.symbolTableDependencies
}

trait AggregationMapper {
  def map(data: MorselExecutionContext, state: OldQueryState): Unit
  def result: AnyValue
}

trait AggregationReducer {
  def reduce(value: AnyValue): Unit
  def result: AnyValue
}
