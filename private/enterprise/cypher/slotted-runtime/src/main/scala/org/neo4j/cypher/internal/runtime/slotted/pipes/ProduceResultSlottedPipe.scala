/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class ProduceResultSlottedPipe(source: Pipe, columns: Seq[(String, Expression)])
                                   (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {

  columns.map(_._2).foreach(_.registerOwningPipe(this))

  private val columnExpressionArray = columns.map(_._2).toArray

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats
    if (state.prePopulateResults)
      input.map {
        original =>
          produceAndPopulate(original, state)
          original
      }
    else
      input.map {
        original =>
          produce(original, state)
          original
      }
  }

  private def produceAndPopulate(original: ExecutionContext, state: QueryState): Unit = {
    val subscriber = state.subscriber
    var i = 0
    subscriber.onRecord()
    while (i < columnExpressionArray.length) {
      val value = columnExpressionArray(i)(original, state)
      ValuePopulation.populate(value)
      subscriber.onField(i, value)
      i += 1
    }
    subscriber.onRecordCompleted()
  }

  private def produce(original: ExecutionContext, state: QueryState): Unit = {
    val subscriber = state.subscriber
    var i = 0
    subscriber.onRecord()
    while (i < columnExpressionArray.length) {
      subscriber.onField(i, columnExpressionArray(i)(original, state))
      i += 1
    }
    subscriber.onRecordCompleted()
  }
}