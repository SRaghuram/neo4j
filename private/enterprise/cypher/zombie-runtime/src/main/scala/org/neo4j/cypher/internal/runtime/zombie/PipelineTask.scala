/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
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

  override final def executeWorkUnit(resources: QueryResources, workUnitEvent: WorkUnitEvent): PreparedOutput = {
    if (_output == null) {
      _output = pipelineState.allocateMorsel(workUnitEvent, state)
      executeStartOperators(resources)
    }
    executeOutputOperator(resources)
  }

  private def executeStartOperators(resources: QueryResources): Unit = {
    startTask.operate(_output, queryContext, state, resources)
    for (op <- middleTasks) {
      _output.resetToFirstRow()
      op.operate(_output, queryContext, state, resources)
    }
    _output.resetToFirstRow()
  }

  private def executeOutputOperator(resources: QueryResources): PreparedOutput = {
    val preparedOutput = outputOperatorState.prepareOutput(_output, queryContext, state, resources)
    if (!outputOperatorState.canContinue) {
      // There is no continuation on the output operator,
      // next-time around we need a new output morsel
      _output = null
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
