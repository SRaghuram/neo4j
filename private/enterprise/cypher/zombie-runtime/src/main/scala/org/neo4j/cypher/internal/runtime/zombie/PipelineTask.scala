/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{Morsel, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.zombie.operators.{ContinuableOperatorTask, OperatorTask, OutputOperatorState, PreparedOutput}

/**
  * The [[Task]] of executing an [[ExecutablePipeline]] once.
  *
  * @param startTask  task for executing the start operator
  * @param state  the current QueryState
  */
case class PipelineTask(startTask: ContinuableOperatorTask,
                        middleTasks: Array[OperatorTask],
                        outputOperatorState: OutputOperatorState,
                        queryContext: QueryContext,
                        state: QueryState,
                        pipelineState: PipelineState)
  extends Task[QueryResources] {

  @volatile private var _output: MorselExecutionContext = _

  // TODO this is awkward, let's see if tasks can create their own output, possibly via provided lambda
  def getOrAllocateMorsel(producingWorkUnitEvent: WorkUnitEvent): MorselExecutionContext = {
    if (_output != null) {
      _output
    } else {
      val slots = pipelineState.pipeline.slots
      val slotSize = slots.size()
      val morsel = Morsel.create(slots, state.morselSize)
      new MorselExecutionContext(
        morsel,
        slotSize.nLongs,
        slotSize.nReferences,
        state.morselSize,
        currentRow = 0,
        slots,
        producingWorkUnitEvent)
    }
  }

  override final def executeWorkUnit(resources: QueryResources, output: MorselExecutionContext): PreparedOutput = {
    /*

    TODO less vomit, also comment

    _(´ཀ`」 ∠)_

         {)
      ,;`/Y\
    ,:;  /^\

     */
    if (_output == null) {
      executeStartOperators(resources, output)
    }
    executeOutputOperator(resources, output)
  }

  private def executeStartOperators(resources: QueryResources, output: MorselExecutionContext): Unit = {
    startTask.operate(output, queryContext, state, resources)
    for (op <- middleTasks) {
      output.resetToFirstRow()
      op.operate(output, queryContext, state, resources)
    }
    output.resetToFirstRow()
  }

  private def executeOutputOperator(resources: QueryResources, output: MorselExecutionContext): PreparedOutput = {
    val preparedOutput = outputOperatorState.prepareOutput(output, queryContext, state, resources)
    if (outputOperatorState.canContinue) {
      // TODO comment
      _output = output
    }
    preparedOutput
  }

  /**
    * Remove everything related to cancelled argumentRowIds from to the task's input.
    *
    * @return `true` if the task has become obsolete.
    */
  def filterCancelledArguments(): Boolean = {
    startTask.filterCancelledArguments(pipelineState)
  }

  /**
    * Close resources related to this task and update relevant counts.
    */
  def close(): Unit = {
    startTask.close(pipelineState)
  }

  override def workId: Int = pipelineState.pipeline.workId

  override def workDescription: String = pipelineState.pipeline.workDescription

  override def canContinue: Boolean = startTask.canContinue || outputOperatorState.canContinue
}
