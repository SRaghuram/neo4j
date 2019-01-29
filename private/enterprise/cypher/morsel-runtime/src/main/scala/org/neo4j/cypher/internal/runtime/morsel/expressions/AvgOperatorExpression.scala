/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, AvgFunction}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.v4_0.util.symbols.{AnyType, CTAny}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.{ListValue, VirtualValues}

/*
Vectorized version of the average aggregation function
 */
case class AvgOperatorExpression(innerMapperExpression: Expression) extends AggregationExpressionOperatorWithInnerExpression(innerMapperExpression) {

  override def expectedInnerType: AnyType = CTAny

  override def rewrite(f: Expression => Expression): Expression = f(AvgOperatorExpression(innerMapperExpression.rewrite(f)))

  override def createAggregationMapper: AggregationFunction = new AvgMapper(new AvgFunction(innerMapperExpression))

  override def createAggregationReducer(expression: Expression): AggregationFunction = new AvgReducer(expression)
}

class AvgMapper(func: AvgFunction) extends AggregationFunction {

  override def result(state: OldQueryState): AnyValue = VirtualValues.list(longValue(func.aggregatedRowCount), func.result(state))

  override def apply(data: ExecutionContext, state: OldQueryState): Unit = func(data, state)
}

class AvgReducer(expression: Expression) extends AggregationFunction {

  private val func: AvgFunction = new AvgFunction(expression)

  override def result(state: OldQueryState): AnyValue = func.result(state)

  override def apply(data: ExecutionContext, state: OldQueryState): Unit = expression(data, state) match {
    case l: ListValue =>
      val weightOfMorsel = l.value(0).asInstanceOf[LongValue].longValue()
      val avgOfMorsel = l.value(1)
      for (_ <- 1L to weightOfMorsel) { func.applyValueDirectly(avgOfMorsel) }
    case x =>
      throw new IllegalStateException(s"Unexpected value in avg reducer: $x")
  }
}
