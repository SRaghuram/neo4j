/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.parallel.{Task, WorkIdentity}
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

abstract class AbstractPipelineTask(operators: IndexedSeq[OperatorTask],
                                    slots: SlotConfiguration,
                                    workIdentity: WorkIdentity,
                                    originalQueryContext: QueryContext,
                                    state: QueryState,
                                    downstream: Option[Pipeline]) extends Task[ExpressionCursors] {

  def ownerPipeline: Pipeline
  def pipelineArgument: PipelineArgument

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

  protected def getDownstreamTasks(cursors: ExpressionCursors, outputRow: MorselExecutionContext, queryContext: QueryContext): Seq[Task[ExpressionCursors]] = {
    outputRow.resetToFirstRow()
    val downstreamTasks = downstream.map(_.acceptMorsel(outputRow, queryContext, state, cursors, pipelineArgument, this)).getOrElse(Seq.empty)

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG && downstreamTasks.nonEmpty) {
      dprintln(() => s">>> downstream tasks=$downstreamTasks")
    }

    // REV: I think that this reduceCollector can never be None or else there may be nothing tracking that we have completed when we get no more tasks
    state.reduceCollector match {
      case Some(x) if !canContinue =>
        // Signal the downstream reduce collector that we have completed. This may produce new tasks from the
        // downstream reduce pipeline if it has received enough completed tasks to proceed.
        val downstreamReduceTasks = x.produceTaskCompleted(queryContext, state, cursors)
        downstreamTasks ++ downstreamReduceTasks

      case _ =>
        downstreamTasks
    }
  }

  override def workId: Int = workIdentity.workId

  override def workDescription: String = workIdentity.workDescription
}

/**
  * The [[Task]] of executing a [[Pipeline]] once.
  *
  * @param start                task for executing the start operator
  * @param operators            the subsequent [[OperatorTask]]s
  * @param slots                the slotConfiguration of this Pipeline
  * @param workIdentity         description of computation performed by this task
  * @param originalQueryContext the query context
  * @param state                the current QueryState
  * @param pipelineArgument     an argument passed to this task
  * @param ownerPipeline        the Pipeline from where this task originated
  * @param downstream           the downstream Pipeline
  */
case class PipelineTask(start: ContinuableOperatorTask,
                        operators: IndexedSeq[OperatorTask],
                        slots: SlotConfiguration,
                        workIdentity: WorkIdentity,
                        originalQueryContext: QueryContext,
                        state: QueryState,
                        pipelineArgument: PipelineArgument,
                        ownerPipeline: Pipeline,
                        downstream: Option[Pipeline])
  extends AbstractPipelineTask(operators, slots, workIdentity, originalQueryContext, state, downstream) {

  override def executeWorkUnit(cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
    val outputMorsel = Morsel.create(slots, state.morselSize)
    val outputRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, state.morselSize, 0, slots)
    val queryContext = getQueryContext

    start.operate(outputRow, queryContext, state, cursors)

    doStatelessOperators(cursors, outputRow, queryContext)

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG) {
      dprintln(() => s"Pipeline: $toString")

      val longCount = slots.numberOfLongs
      val refCount = slots.numberOfReferences

      dprintln(() => "Resulting rows")
      for (i <- 0 until outputRow.getValidRows) {
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
  * @param workIdentity         description of computation performed by this task
  * @param originalQueryContext the query context
  * @param state                the current QueryState
  * @param pipelineArgument     an argument passed to this task
  * @param ownerPipeline        the Pipeline from where this task originated
  * @param downstream           the downstream Pipeline
  */
case class LazyReducePipelineTask(start: LazyReduceOperatorTask,
                                  operators: IndexedSeq[OperatorTask],
                                  slots: SlotConfiguration,
                                  workIdentity: WorkIdentity,
                                  originalQueryContext: QueryContext,
                                  state: QueryState,
                                  pipelineArgument: PipelineArgument,
                                  ownerPipeline: Pipeline,
                                  downstream: Option[Pipeline])
  extends AbstractPipelineTask(operators, slots, workIdentity, originalQueryContext, state, downstream) {

  override def executeWorkUnit(cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
    val queryContext = getQueryContext
    val morsels = start.operate(queryContext, state, cursors)
    morsels.foreach(doStatelessOperators(cursors, _, queryContext))
    morsels.flatMap(getDownstreamTasks(cursors, _, queryContext))
  }

  override def canContinue: Boolean = false
}
