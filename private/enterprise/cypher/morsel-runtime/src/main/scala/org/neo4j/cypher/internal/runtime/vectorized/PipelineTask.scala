/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.{Task, WorkIdentity}
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

abstract class AbstractPipelineTask(operators: IndexedSeq[OperatorTask],
                                    slots: SlotConfiguration,
                                    workIdentity: WorkIdentity,
                                    queryContext: QueryContext,
                                    state: QueryState,
                                    downstream: Option[Pipeline]) extends Task[QueryResources] {

  def ownerPipeline: Pipeline
  def pipelineArgument: PipelineArgument
  def doExecuteWorkUnit(cursors: QueryResources): Seq[Task[QueryResources]]

  override final def executeWorkUnit(expressionCursors: QueryResources): Seq[Task[QueryResources]] = {
    try {
      state.transactionBinder.bindToThread(queryContext.transactionalContext.transaction)
      doExecuteWorkUnit(expressionCursors)
    } finally {
      state.transactionBinder.unbindFromThread()
    }
  }

  protected def doStatelessOperators(resources: QueryResources,
                                     outputRow: MorselExecutionContext,
                                     queryContext: QueryContext): Unit = {
    for (op <- operators) {
      outputRow.resetToFirstRow()
      op.operate(outputRow, queryContext, state, resources)
    }
  }

  protected def getDownstreamTasks(resources: QueryResources,
                                   outputRow: MorselExecutionContext,
                                   queryContext: QueryContext): Seq[Task[QueryResources]] = {
    outputRow.resetToFirstRow()
    val downstreamTasks = downstream.map(_.acceptMorsel(outputRow, queryContext, state, resources, pipelineArgument, this)).getOrElse(Seq.empty)

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG && downstreamTasks.nonEmpty) {
      dprintln(() => s">>> downstream tasks=$downstreamTasks")
    }

    // REV: I think that this reduceCollector can never be None or else there may be nothing tracking that we have completed when we get no more tasks
    state.reduceCollector match {
      case Some(x) if !canContinue =>
        // Signal the downstream reduce collector that we have completed. This may produce new tasks from the
        // downstream reduce pipeline if it has received enough completed tasks to proceed.
        val downstreamReduceTasks = x.produceTaskCompleted(queryContext, state, resources)
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
                        queryContext: QueryContext,
                        state: QueryState,
                        pipelineArgument: PipelineArgument,
                        ownerPipeline: Pipeline,
                        downstream: Option[Pipeline])
  extends AbstractPipelineTask(operators, slots, workIdentity, queryContext, state, downstream) {

  override def doExecuteWorkUnit(resources: QueryResources): Seq[Task[QueryResources]] = {
    val outputMorsel = Morsel.create(slots, state.morselSize)
    val outputRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, state.morselSize, 0, slots)

    start.operate(outputRow, queryContext, state, resources)

    doStatelessOperators(resources, outputRow, queryContext)

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

    getDownstreamTasks(resources, outputRow, queryContext)
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
                                  queryContext: QueryContext,
                                  state: QueryState,
                                  pipelineArgument: PipelineArgument,
                                  ownerPipeline: Pipeline,
                                  downstream: Option[Pipeline])
  extends AbstractPipelineTask(operators, slots, workIdentity, queryContext, state, downstream) {

  override def doExecuteWorkUnit(resources: QueryResources): Seq[Task[QueryResources]] = {
    val morsels = start.operate(queryContext, state, resources)
    morsels.foreach(doStatelessOperators(resources, _, queryContext))
    morsels.flatMap(getDownstreamTasks(resources, _, queryContext))
  }

  override def canContinue: Boolean = false
}
