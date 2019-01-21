/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.values.storable.Values
import org.neo4j.values.{AnyValue, AnyValues}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny

/*
Vectorized version of the min and max aggregation functions
 */
abstract class MinOrMaxOperatorExpression(expression: Expression)
  extends AggregationExpressionOperatorWithInnerExpression(expression) {

  override def expectedInnerType = CTAny
}

case class MinOperatorExpression(expression: Expression) extends MinOrMaxOperatorExpression(expression) {
  override def createAggregationMapper: AggregationMapper = new MinMapper(expression)
  override def createAggregationReducer: AggregationReducer = new MinReducer
  override def rewrite(f: (Expression) => Expression): Expression = f(MinOperatorExpression(expression.rewrite(f)))

}

case class MaxOperatorExpression(expression: Expression) extends MinOrMaxOperatorExpression(expression) {
  override def createAggregationMapper: AggregationMapper = new MaxMapper(expression)
  override def createAggregationReducer: AggregationReducer = new MaxReducer
  override def rewrite(f: (Expression) => Expression): Expression = f(MaxOperatorExpression(expression.rewrite(f)))
}

trait MinMaxChecker {
  protected var optimum: AnyValue = Values.NO_VALUE

  def keep(comparisonResult: Int): Boolean

  def result: AnyValue = optimum

  protected def checkIfLargest(value: AnyValue) {
    if (optimum == Values.NO_VALUE) {
      optimum = value
    } else if (keep(AnyValues.COMPARATOR.compare(optimum, value))) {
      optimum = value
    }
  }


   def reduce(value: AnyValue): Unit = value match {
    case Values.NO_VALUE =>
    case value: AnyValue=> checkIfLargest(value)
  }
}

trait MinChecker extends MinMaxChecker {
  override def keep(comparisonResult: Int): Boolean = comparisonResult > 0

}

trait MaxChecker extends MinMaxChecker {
  override def keep(comparisonResult: Int): Boolean = comparisonResult < 0
}

class MinMapper(value: Expression) extends AggregationMapper with MinChecker {

  def map(data: MorselExecutionContext,
          state: OldQueryState): Unit = reduce(value(data, state))
}

class MinReducer extends AggregationReducer with MinChecker

class MaxMapper(value: Expression) extends AggregationMapper with MaxChecker {

  def map(data: MorselExecutionContext,
          state: OldQueryState): Unit = reduce(value(data, state))
}

class MaxReducer extends AggregationReducer with MaxChecker








