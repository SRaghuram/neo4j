/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.{ContinuableOperatorTask, OperatorTask, OutputOperatorState, PreparedOutput}
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

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

  /**
    * This _output reference is needed to support reactive results in produce results,
    * and in particular for ProduceResultsOperator to leave continuations. So if all
    * demand is met before we have produces all output, the _output morsel will be != null,
    * and the next work unit of this task will continue produce output off that _output,
    * and not do any other work.
    *
    * It is important the all previous output is produced before continuing on any input,
    * in order to retain the produced row order. Also we can never cancel a task with
    * unprocessed _output.
    */
  private var _output: MorselExecutionContext = _

  override final def executeWorkUnit(resources: QueryResources,
                                     workUnitEvent: WorkUnitEvent,
                                     queryProfiler: QueryProfiler): PreparedOutput = {
    if (_output == null) {
      _output = pipelineState.allocateMorsel(workUnitEvent, state)
      executeOperators(resources, queryProfiler)
    }
    executeOutputOperator(resources, queryProfiler)
  }

  private def executeOperators(resources: QueryResources,
                               queryProfiler: QueryProfiler): Unit = {
    startTask.operateWithProfile(_output, queryContext, state, resources, queryProfiler)
    _output.resetToFirstRow()
    for (op <- middleTasks) {
      op.operateWithProfile(_output, queryContext, state, resources, queryProfiler)
      _output.resetToFirstRow()
    }
  }

  private def executeOutputOperator(resources: QueryResources,
                                    queryProfiler: QueryProfiler): PreparedOutput = {
    val preparedOutput = outputOperatorState.prepareOutput(_output, queryContext, state, resources, queryProfiler)
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
  def filterCancelledArguments(resources: QueryResources): Boolean = {
    if (_output == null) {
      val isCancelled = startTask.filterCancelledArguments(pipelineState)
      if (isCancelled) {
        close(resources)
      }
      isCancelled
    } else {
      false
    }
  }

  /**
    * Close resources related to this task and update relevant counts.
    */
  def close(resources: QueryResources): Unit = {
    startTask.close(pipelineState, resources)
  }

  override def workId: Id = pipelineState.pipeline.workId

  override def workDescription: String = pipelineState.pipeline.workDescription

  override def canContinue: Boolean = startTask.canContinue || outputOperatorState.canContinue
}
