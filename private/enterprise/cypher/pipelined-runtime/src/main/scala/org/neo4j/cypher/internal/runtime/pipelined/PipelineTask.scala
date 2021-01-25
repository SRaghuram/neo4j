/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.MemoizingMeasurable
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.OutputOperatorState
import org.neo4j.cypher.internal.runtime.pipelined.operators.PreparedOutput
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * The [[Task]] of executing an [[ExecutablePipeline]] once.
 *
 * @param startTask  task for executing the start operator
 * @param state  the current QueryState
 */
case class PipelineTask(startTask: ContinuableOperatorTask,
                        middleTasks: Array[OperatorTask],
                        outputOperatorState: OutputOperatorState,
                        state: PipelinedQueryState,
                        pipelineState: PipelineState)
  extends Task[QueryResources] with MemoizingMeasurable {

  /**
   * This _output reference is needed to support reactive results in produce results,
   * and in particular for ProduceResultsOperator to leave continuations. So if all
   * demand is met before we have produced all output, the _output morsel will be != null,
   * and the next work unit of this task will continue produce output off that _output,
   * and not do any other work.
   *
   * It is important the all previous output is produced before continuing on any input,
   * in order to retain the produced row order. Also we can never cancel a task with
   * unprocessed _output.
   */
  private[this] var _output: Morsel = _

  override def executeWorkUnit(resources: QueryResources,
                               workUnitEvent: WorkUnitEvent,
                               queryProfiler: QueryProfiler): PreparedOutput = {
    if (_output == null) {
      _output = pipelineState.allocateMorsel(workUnitEvent, state)
      executeOperators(resources, queryProfiler)
    }
    val output = executeOutputOperator(resources, queryProfiler)
    DebugSupport.logPipelines(PipelinedDebugSupport.prettyWorkDone)
    output
  }

  private def executeOperators(resources: QueryResources,
                               queryProfiler: QueryProfiler): Unit = {
    DebugSupport.logPipelines(PipelinedDebugSupport.prettyStartTask(startTask, pipelineState.pipeline.start.workIdentity))
    startTask.operateWithProfile(_output, state, resources, queryProfiler)
    DebugSupport.logPipelines(PipelinedDebugSupport.prettyPostStartTask(startTask))
    var i = 0
    while (i < middleTasks.length) {
      val op = middleTasks(i)
      DebugSupport.logPipelines(PipelinedDebugSupport.prettyWork(_output, pipelineState.pipeline.middleOperators(i).workIdentity))
      op.operateWithProfile(_output, state, resources, queryProfiler)
      i += 1
    }
  }

  private def executeOutputOperator(resources: QueryResources,
                                    queryProfiler: QueryProfiler): PreparedOutput = {
    DebugSupport.logPipelines(PipelinedDebugSupport.prettyWork(_output, pipelineState.pipeline.outputOperator.workIdentity))
    val preparedOutput = outputOperatorState.prepareOutputWithProfile(_output, state, resources, queryProfiler)
    if (!outputOperatorState.canContinueOutput) {
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

  override def canContinue: Boolean = startTask.canContinue || outputOperatorState.canContinueOutput

  override def estimatedHeapUsage: Long = startTask.estimatedHeapUsage
}
