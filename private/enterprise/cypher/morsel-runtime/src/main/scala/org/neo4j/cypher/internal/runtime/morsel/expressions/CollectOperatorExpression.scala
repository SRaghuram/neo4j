/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.{AggregationFunction, CollectFunction}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.v4_0.util.symbols.{AnyType, CTAny}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, VirtualValues}

import scala.collection.mutable.ArrayBuffer

/*
Vectorized version of the collect aggregation function
 */
case class CollectOperatorExpression(innerMapperExpression: Expression) extends AggregationExpressionOperatorWithInnerExpression(innerMapperExpression) {

  override def expectedInnerType: AnyType = CTAny

  override def rewrite(f: Expression => Expression): Expression = f(CollectOperatorExpression(innerMapperExpression.rewrite(f)))

  override def createAggregationMapper: AggregationFunction = new CollectFunction(innerMapperExpression)

  override def createAggregationReducer(expression: Expression): AggregationFunction = new CollectReducer(expression)
}

private class CollectReducer(expression: Expression) extends AggregationFunction {
  private val collections = ArrayBuffer[ListValue]()

  //TODO this is not very efficient, we could use a specialized concatenated
  //ListValue that wraps a list instead of an array
  override def result(state: OldQueryState): AnyValue = VirtualValues.concat(collections.toArray:_*)
  override def apply(data: ExecutionContext, state: OldQueryState): Unit = expression(data, state) match {
    case l: ListValue => collections.append(l)
    case _ => throw new IllegalStateException()
  }
}




