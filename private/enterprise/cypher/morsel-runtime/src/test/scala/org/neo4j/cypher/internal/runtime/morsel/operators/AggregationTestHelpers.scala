/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel.expressions.AggregationExpressionOperator
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{LongArray, Values}

import scala.collection.mutable

//Dummy aggregation, for test only
case class DummyEvenNodeIdAggregation(offset: Int) extends AggregationExpressionOperator {

  override def createAggregationMapper: AggregationFunction = new EvenNodeIdMapper(offset)

  override def createAggregationReducer(expression: Expression): AggregationFunction = new EvenNodeIdReducer(expression)

  override def rewrite(f: Expression => Expression): Expression = this

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty

  override def children: Seq[AstNode[_]] = Seq.empty
}

class DummyExpression(values: AnyValue*) extends Expression {
  private var current = 0

  override def rewrite(f: Expression => Expression): Expression = this

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty

  override def apply(ctx: ExecutionContext,
                     state: OldQueryState): AnyValue = {
    val next = values(current)
    current = (current + 1) % values.length
    next
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}

private class EvenNodeIdMapper(offset: Int) extends AggregationFunction {

  private val evenNodes = mutable.Set[Long]()

  override def apply(data: ExecutionContext, ignore: OldQueryState): Unit = {
    val id = data.getLongAt(offset)
    if (id % 2 == 0) evenNodes.add(id)
  }

  override def result(state: OldQueryState): AnyValue = Values.longArray(evenNodes.toArray.sorted)
}

private class EvenNodeIdReducer(expression: Expression) extends AggregationFunction {

  private val evenNodes = mutable.Set[Long]()

  override def apply(data: ExecutionContext, state: OldQueryState): Unit = expression(data, state) match {
    case ls: LongArray => ls.asObjectCopy().foreach(evenNodes.add)
  }

  override def result(state: OldQueryState): AnyValue = Values.longArray(evenNodes.toArray.sorted)
}

