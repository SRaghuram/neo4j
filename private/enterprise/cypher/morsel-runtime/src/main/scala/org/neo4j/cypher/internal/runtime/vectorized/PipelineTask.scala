/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.parallel.Task
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext

/**
  * The [[Task]] of executing a [[Pipeline]] once.
  *
  * @param start task for executing the start operator
  * @param operators the subsequent [[OperatorTask]]s
  * @param slots the slotConfiguration of this Pipeline
  * @param name name of this task
  * @param originalQueryContext the query context
  * @param state the current QueryState
  * @param downstream the downstream Pipeline
  */
case class PipelineTask(start: ContinuableOperatorTask,
                        operators: IndexedSeq[OperatorTask],
                        slots: SlotConfiguration,
                        name: String,
                        originalQueryContext: QueryContext,
                        state: QueryState,
                        downstream: Option[Pipeline]) extends Task[ExpressionCursors] {

  override def executeWorkUnit(cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
    val outputMorsel = Morsel.create(slots, state.morselSize)
    val currentRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, 0)
    val queryContext =
      if (state.singeThreaded) originalQueryContext
      else originalQueryContext.createNewQueryContext()
    start.operate(currentRow, queryContext, state, cursors)

    for (op <- operators) {
      currentRow.resetToFirstRow()
      op.operate(currentRow, queryContext, state, cursors)
    }

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG) {
      println(s"Pipeline: $name")

      val longCount = slots.numberOfLongs
      val refCount = slots.numberOfReferences

      println("Resulting rows")
      for (i <- 0 until outputMorsel.validRows) {
        val ls =  util.Arrays.toString(outputMorsel.longs.slice(i * longCount, (i + 1) * longCount))
        val rs =  util.Arrays.toString(outputMorsel.refs.slice(i * refCount, (i + 1) * refCount).asInstanceOf[Array[AnyRef]])
        println(s"$ls $rs")
      }
      println(s"can continue: ${start.canContinue}")
      println()
      println("-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/")
    }

    currentRow.resetToFirstRow()
    val maybeDownsteamTask = downstream.flatMap(_.acceptMorsel(currentRow, queryContext, state, cursors))

    state.reduceCollector match {
      case Some(x) if !start.canContinue =>
        maybeDownsteamTask.toSeq ++ x.produceTaskCompleted(name, queryContext, state, cursors)

      case _ =>
        maybeDownsteamTask.toSeq
    }
  }

  override def canContinue: Boolean = start.canContinue

  override def toString: String = name
}
