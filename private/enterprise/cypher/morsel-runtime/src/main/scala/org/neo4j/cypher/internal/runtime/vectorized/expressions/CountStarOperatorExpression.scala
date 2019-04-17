/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{NumberValue, Values}

/*
Vectorized version of the count star aggregation function
 */
case object CountStarOperatorExpression extends AggregationExpressionOperator {

  override def createAggregationMapper: AggregationMapper = new CountStarMapperAndReducer

  override def createAggregationReducer: AggregationReducer = new CountStarMapperAndReducer

  override def rewrite(f: (Expression) => Expression): Expression = f(CountStarOperatorExpression)

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty

  override def children: Seq[AstNode[_]] = Seq.empty
}

private class CountStarMapperAndReducer extends AggregationMapper with AggregationReducer {
  private var count: Long = 0L

  override def result: AnyValue = Values.longValue(count)
  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit =  {
    count += 1L
  }

  override def reduce(value: AnyValue): Unit = value match {
    case l: NumberValue => count += l.longValue()
    case _ =>
  }
}



