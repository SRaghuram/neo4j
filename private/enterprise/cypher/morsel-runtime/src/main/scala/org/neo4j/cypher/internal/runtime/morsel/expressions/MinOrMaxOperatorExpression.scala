/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, MaxFunction, MinFunction}
import org.neo4j.cypher.internal.v4_0.util.symbols.{AnyType, CTAny}

/*
Vectorized version of the min and max aggregation functions
 */
abstract class MinOrMaxOperatorExpression(innerMapperExpression: Expression)
  extends AggregationExpressionOperatorWithInnerExpression(innerMapperExpression) {

  override def expectedInnerType: AnyType = CTAny
}

case class MinOperatorExpression(innerMapperExpression: Expression) extends MinOrMaxOperatorExpression(innerMapperExpression) {
  override def createAggregationMapper: AggregationFunction = new MinFunction(innerMapperExpression)
  override def createAggregationReducer(expression: Expression): AggregationFunction = new MinFunction(expression)
  override def rewrite(f: Expression => Expression): Expression = f(MinOperatorExpression(innerMapperExpression.rewrite(f)))
}

case class MaxOperatorExpression(innerMapperExpression: Expression) extends MinOrMaxOperatorExpression(innerMapperExpression) {
  override def createAggregationMapper: AggregationFunction = new MaxFunction(innerMapperExpression)
  override def createAggregationReducer(expression: Expression): AggregationFunction = new MaxFunction(expression)
  override def rewrite(f: Expression => Expression): Expression = f(MaxOperatorExpression(innerMapperExpression.rewrite(f)))
}
