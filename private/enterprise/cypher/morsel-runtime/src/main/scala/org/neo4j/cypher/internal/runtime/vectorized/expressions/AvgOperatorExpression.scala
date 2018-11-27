/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.{LongValue, NumberValue, Values}
import org.neo4j.values.utils.ValueMath.overflowSafeAdd
import org.neo4j.values.virtual.{ListValue, VirtualValues}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny

/*
Vectorized version of the average aggregation function
 */
case class AvgOperatorExpression(anInner: Expression) extends AggregationExpressionOperatorWithInnerExpression(anInner) {

  override def expectedInnerType = CTAny

  override def rewrite(f: (Expression) => Expression): Expression = f(AvgOperatorExpression(anInner.rewrite(f)))

  override def createAggregationMapper: AggregationMapper = new AvgMapper(anInner)

  override def createAggregationReducer: AggregationReducer = new AvgReducer
}

class AvgMapper(value: Expression) extends AggregationMapper {

  private var count: Long = 0L
  private var sum: NumberValue = Values.ZERO_INT

  override def result: AnyValue = VirtualValues.list(longValue(count), sum)

  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit = value(data, state) match {
    case Values.NO_VALUE =>
    case number: NumberValue =>
      count += 1
      sum = overflowSafeAdd(sum, number);
  }
}

class AvgReducer extends AggregationReducer {

  private var count: Long = 0L
  private var sum: NumberValue = longValue(0L)

  override def result: AnyValue = if (count > 0L) sum.times(1.0 / count.toDouble) else Values.NO_VALUE

  override def reduce(value: AnyValue): Unit = value match {
    case l: ListValue =>
      count += l.value(0).asInstanceOf[LongValue].longValue()
      sum = overflowSafeAdd(sum, l.value(1).asInstanceOf[NumberValue]);
    case _ =>
  }
}



