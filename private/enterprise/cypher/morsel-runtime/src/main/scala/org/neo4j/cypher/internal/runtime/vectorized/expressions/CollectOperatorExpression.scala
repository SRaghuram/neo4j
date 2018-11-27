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
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{ListValue, VirtualValues}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny

import scala.collection.mutable.ArrayBuffer

/*
Vectorized version of the collect aggregation function
 */
case class CollectOperatorExpression(anInner: Expression) extends AggregationExpressionOperatorWithInnerExpression(anInner) {

  override def expectedInnerType = CTAny

  override def rewrite(f: (Expression) => Expression): Expression = f(CollectOperatorExpression(anInner.rewrite(f)))

  override def createAggregationMapper: AggregationMapper = new CollectMapper(anInner)

  override def createAggregationReducer: AggregationReducer = new CollectReducer
}

private class CollectMapper(value: Expression) extends AggregationMapper {
  val collection = new java.util.ArrayList[AnyValue]()

  override def result: AnyValue = VirtualValues.fromList(collection)

  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit =  value(data, state) match {
    case Values.NO_VALUE =>
    case v   => collection.add(v)
  }
}

private class CollectReducer extends AggregationReducer {
  private val collections = ArrayBuffer[ListValue]()

  //TODO this is not very efficient, we could use a specialized concatenated
  //ListValue that wraps a list instead of an array
  override def result: AnyValue = VirtualValues.concat(collections.toArray:_*)
  override def reduce(value: AnyValue): Unit = value match {
    case l: ListValue => collections.append(l)
    case _ => throw new IllegalStateException()
  }
}




