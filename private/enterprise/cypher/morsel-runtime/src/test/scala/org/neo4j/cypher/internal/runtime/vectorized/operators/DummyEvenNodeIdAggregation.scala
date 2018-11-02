/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

;

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.vectorized.expressions.{AggregationExpressionOperator, AggregationMapper, AggregationReducer}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{LongArray, Values}

import scala.collection.mutable

//Dummy aggregation, for test only
case class DummyEvenNodeIdAggregation(offset: Int) extends AggregationExpressionOperator {

  override def createAggregationMapper: AggregationMapper = new EvenNodeIdMapper(offset)

  override def createAggregationReducer: AggregationReducer = new EvenNodeIdReducer

  override def rewrite(f: (Expression) => Expression): Expression = ???

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty
}

class DummyExpression(values: AnyValue*) extends Expression {
  private var current = 0

  override def rewrite(f: (Expression) => Expression): Expression = this

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty

  override def apply(ctx: ExecutionContext,
                     state: OldQueryState): AnyValue = {
    val next = values(current)
    current = (current + 1) % values.length
    next
  }
}

private class EvenNodeIdMapper(offset: Int) extends AggregationMapper {

  private val evenNodes = mutable.Set[Long]()

  override def map(data: MorselExecutionContext, ignore: OldQueryState): Unit = {
    val id = data.getLongAt(offset)
    if (id % 2 == 0) evenNodes.add(id)
  }

  override def result: AnyValue = Values.longArray(evenNodes.toArray.sorted)
}

private class EvenNodeIdReducer extends AggregationReducer {

  private val evenNodes = mutable.Set[Long]()

  override def reduce(value: AnyValue): Unit = value match {
    case ls: LongArray => ls.asObjectCopy().foreach(evenNodes.add)
  }

  override def result: AnyValue = Values.longArray(evenNodes.toArray.sorted)
}

