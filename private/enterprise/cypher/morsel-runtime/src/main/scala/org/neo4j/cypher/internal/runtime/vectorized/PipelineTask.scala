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
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

abstract class AbstractPipelineTask(operators: IndexedSeq[OperatorTask],
                                    slots: SlotConfiguration,
                                    name: String,
                                    originalQueryContext: QueryContext,
                                    state: QueryState,
                                    downstream: Option[Pipeline]) extends Task[ExpressionCursors] {

  def ownerPipeline: Pipeline

  protected def getQueryContext: QueryContext = {
    if (state.singeThreaded) {
      originalQueryContext
    } else {
      originalQueryContext.createNewQueryContext()
    }
  }

  protected def doStatelessOperators(cursors: ExpressionCursors, outputRow: MorselExecutionContext, queryContext: QueryContext): Unit = {
    for (op <- operators) {
      outputRow.resetToFirstRow()
      op.operate(outputRow, queryContext, state, cursors)
    }
  }

  protected def getDownstreamTasks(cursors: ExpressionCursors, currentRow: MorselExecutionContext, queryContext: QueryContext): Seq[Task[ExpressionCursors]] = {
    currentRow.resetToFirstRow()
    val downstreamTasks = downstream.flatMap(_.acceptMorsel(currentRow, queryContext, state, cursors, this)).toSeq

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG && downstreamTasks.nonEmpty) {
      dprintln(() => s">>> downstream tasks=$downstreamTasks")
    }

    state.reduceCollector match {
      case Some(x) if !canContinue =>
        downstreamTasks ++ x.produceTaskCompleted(name, queryContext, state, cursors)

      case _ =>
        downstreamTasks
    }
  }
}

/**
  * The [[Task]] of executing a [[Pipeline]] once.
  *
  * @param start                task for executing the start operator
  * @param operators            the subsequent [[OperatorTask]]s
  * @param slots                the slotConfiguration of this Pipeline
  * @param name                 name of this task
  * @param originalQueryContext the query context
  * @param state                the current QueryState
  * @param ownerPipeline        the Pipeline from where this task originated
  * @param downstream           the downstream Pipeline
  */
case class PipelineTask(start: ContinuableOperatorTask,
                        operators: IndexedSeq[OperatorTask],
                        slots: SlotConfiguration,
                        name: String,
                        originalQueryContext: QueryContext,
                        state: QueryState,
                        ownerPipeline: Pipeline,
                        downstream: Option[Pipeline])
  extends AbstractPipelineTask(operators, slots, name, originalQueryContext, state, downstream) {

  override def executeWorkUnit(cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
    val outputMorsel = Morsel.create(slots, state.morselSize)
    val outputRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, 0)
    val queryContext = getQueryContext

    start.operate(outputRow, queryContext, state, cursors)

    doStatelessOperators(cursors, outputRow, queryContext)

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG) {
      dprintln(() => s"Pipeline: $name")

      val longCount = slots.numberOfLongs
      val refCount = slots.numberOfReferences

      dprintln(() => "Resulting rows")
      for (i <- 0 until outputMorsel.validRows) {
        val ls = util.Arrays.toString(outputMorsel.longs.slice(i * longCount, (i + 1) * longCount))
        val rs = util.Arrays.toString(outputMorsel.refs.slice(i * refCount, (i + 1) * refCount).asInstanceOf[Array[AnyRef]])
        println(s"$ls $rs")
      }
      dprintln(() => s"can continue: ${start.canContinue}")
      println()
      dprintln(() => "-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/")
    }

    getDownstreamTasks(cursors, outputRow, queryContext)
  }

  override def canContinue: Boolean = start.canContinue

  override def toString: String = name
}

/**
  * The [[Task]] of executing a [[LazyReducePipeline]].
  *
  * This can be created multiple times per [[LazyReducePipeline]], depending on the scheduling.
  * It will get called once for every [[LazyReduceOperatorTask]] that is created.
  *
  * @param start                task for executing the start operator
  * @param operators            the subsequent [[OperatorTask]]s
  * @param slots                the slotConfiguration of this Pipeline
  * @param name                 name of this task
  * @param originalQueryContext the query context
  * @param state                the current QueryState
  * @param ownerPipeline        the Pipeline from where this task originated
  * @param downstream           the downstream Pipeline
  */
case class LazyReducePipelineTask(start: LazyReduceOperatorTask,
                                  operators: IndexedSeq[OperatorTask],
                                  slots: SlotConfiguration,
                                  name: String,
                                  originalQueryContext: QueryContext,
                                  state: QueryState,
                                  ownerPipeline: Pipeline,
                                  downstream: Option[Pipeline])
  extends AbstractPipelineTask(operators, slots, name, originalQueryContext, state, downstream) {

  override def executeWorkUnit(cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
    val queryContext = getQueryContext
    val morsels = start.operate(queryContext, state, cursors)
    morsels.foreach(doStatelessOperators(cursors, _, queryContext))
    morsels.flatMap(getDownstreamTasks(cursors, _, queryContext))
  }

  override def canContinue: Boolean = false

  override def toString: String = name
}
