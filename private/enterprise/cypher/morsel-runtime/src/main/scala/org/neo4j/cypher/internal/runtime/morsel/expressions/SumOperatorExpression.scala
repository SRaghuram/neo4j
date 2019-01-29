/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, SumFunction}
import org.neo4j.cypher.internal.v4_0.util.symbols.{AnyType, CTAny}

/**
  * Vectorized version of the sum aggregation function
  */
case class SumOperatorExpression(innerMapperExpression: Expression) extends AggregationExpressionOperatorWithInnerExpression(innerMapperExpression) {

  override def expectedInnerType: AnyType = CTAny

  override def rewrite(f: Expression => Expression): Expression = f(SumOperatorExpression(innerMapperExpression.rewrite(f)))

  override def createAggregationMapper: AggregationFunction = new SumFunction(innerMapperExpression)

  override def createAggregationReducer(expression: Expression): AggregationFunction = new SumFunction(expression)
}
