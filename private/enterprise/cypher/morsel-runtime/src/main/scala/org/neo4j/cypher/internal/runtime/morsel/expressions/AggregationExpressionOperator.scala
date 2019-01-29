/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.v4_0.util.SyntaxException
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType

abstract class AggregationExpressionOperator extends Expression {

  def apply(ctx: ExecutionContext, state: OldQueryState) =
    throw new UnsupportedOperationException("Aggregations should not be used like this.")

  def createAggregationMapper: AggregationFunction
  def createAggregationReducer(expression: Expression): AggregationFunction
}

/**
  * Aggregation expression in morsel that builds upon an inner expression.
  *
  * @param innerMapperExpression    the actual expression to aggregate on.
  */
abstract class AggregationExpressionOperatorWithInnerExpression(innerMapperExpression: Expression) extends AggregationExpressionOperator {
  if(innerMapperExpression.containsAggregate)
    throw new SyntaxException("Can't use aggregate functions inside of aggregate functions.")

  if(! innerMapperExpression.isDeterministic)
    throw new SyntaxException("Can't use non-deterministic (random) functions inside of aggregate functions.")

  def expectedInnerType: CypherType

  def arguments: Seq[Expression] = Seq(innerMapperExpression)

  def symbolTableDependencies: Set[String] = innerMapperExpression.symbolTableDependencies
}
