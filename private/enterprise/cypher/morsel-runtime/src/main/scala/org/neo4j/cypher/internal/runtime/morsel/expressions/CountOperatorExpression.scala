/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{NumberValue, Values}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny

/*
Vectorized version of the count aggregation function
 */
case class CountOperatorExpression(anInner: Expression) extends AggregationExpressionOperatorWithInnerExpression(anInner) {

  override def expectedInnerType = CTAny

  override def rewrite(f: (Expression) => Expression): Expression = f(CountOperatorExpression(anInner.rewrite(f)))

  override def createAggregationMapper: AggregationMapper = new CountMapper(anInner)

  override def createAggregationReducer: AggregationReducer = new CountReducer
}

private class CountMapper(value: Expression) extends AggregationMapper {
  private var count: Long = 0L

  override def result: AnyValue = Values.longValue(count)
  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit =  value(data, state) match {
    case Values.NO_VALUE =>
    case _    => count += 1
  }
}

private class CountReducer extends AggregationReducer {
  private var count: Long = 0L

  override def result: AnyValue = Values.longValue(count)
  override def reduce(value: AnyValue): Unit = value match {
    case l: NumberValue => count += l.longValue()
    case _ =>
  }
}
