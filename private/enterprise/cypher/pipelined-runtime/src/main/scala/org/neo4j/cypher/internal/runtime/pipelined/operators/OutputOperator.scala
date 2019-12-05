/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.{OperatorProfileEvent, QueryProfiler}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.pipelined.{ExecutionState, Task}
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.util.attribution.Id

/**
  * Operator which ends a pipeline, and thus prepares the computed output.
  *
  *    [[OutputOperator]]       immutable and cacheable
  *    [[OutputOperatorState]]  created for every query invocation. References [[ExecutionState]].
  *    [[PreparedOutput]]       the prepared output for a specific [[Task]]. The purpose of this class is
  *                             to encapsulate the result just before we put it in an output-buffer, meaning
  *                             that it could be just the output morsel, or also have intermediate aggregation
  *                             data for example.
  */
trait OutputOperator extends HasWorkIdentity {
  def outputBuffer: Option[BufferId]
  def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState
}

trait OutputOperatorState extends HasWorkIdentity {

  def prepareOutputWithProfile(output: MorselExecutionContext,
                               context: QueryContext,
                               state: QueryState,
                               resources: QueryResources,
                               queryProfiler: QueryProfiler): PreparedOutput = {

    val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId)
    resources.setKernelTracer(operatorExecutionEvent)
    try {
      prepareOutput(output, context, state, resources, operatorExecutionEvent)
    } finally {
      resources.setKernelTracer(null)
      if (operatorExecutionEvent != null) {
        operatorExecutionEvent.close()
      }
    }
  }

  protected def prepareOutput(outputMorsel: MorselExecutionContext,
                              context: QueryContext,
                              state: QueryState,
                              resources: QueryResources,
                              operatorExecutionEvent: OperatorProfileEvent): PreparedOutput

  def canContinueOutput: Boolean = false
}

trait PreparedOutput {
  def produce(): Unit
}

// NO OUTPUT

case object NoOutputOperator extends OutputOperator with OutputOperatorState with PreparedOutput {
  override def outputBuffer: Option[BufferId] = None
  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState = this
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = this

  override def produce(): Unit = ()
  override def workIdentity: WorkIdentity = WorkIdentityImpl(Id.INVALID_ID, "Perform no output")
}

// PIPELINED BUFFER OUTPUT

// we need the the id of the head plan of the next pipeline because that is where er attribute time spent
// during prepare output in profiling.
case class MorselBufferOutputOperator(bufferId: BufferId, nextPipelineHeadPlanId: Id) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(nextPipelineHeadPlanId, s"Output morsel to $bufferId")
  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState =
    MorselBufferOutputState(workIdentity, bufferId, executionState, pipelineId)
}
case class MorselBufferOutputState(override val workIdentity: WorkIdentity,
                                   bufferId: BufferId,
                                   executionState: ExecutionState,
                                   pipelineId: PipelineId) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput =
    MorselBufferPreparedOutput(bufferId, executionState, pipelineId, outputMorsel)
}
case class MorselBufferPreparedOutput(bufferId: BufferId,
                                      executionState: ExecutionState,
                                      pipelineId: PipelineId,
                                      outputMorsel: MorselExecutionContext) extends PreparedOutput {
  override def produce(): Unit =
    executionState.putMorsel(pipelineId, bufferId, outputMorsel)
}

// PIPELINED ARGUMENT STATE BUFFER OUTPUT

// we need the the id of the head plan of the next pipeline because that is where er attribute time spent
// during prepare output in profiling.
case class MorselArgumentStateBufferOutputOperator(bufferId: BufferId, argumentSlotOffset: Int, nextPipelineHeadPlanId: Id) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(nextPipelineHeadPlanId, s"Output morsel grouped by argumentSlot $argumentSlotOffset to $bufferId")
  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState =
    MorselArgumentStateBufferOutputState(workIdentity,
                                         executionState.getSink[IndexedSeq[PerArgument[MorselExecutionContext]]](pipelineId, bufferId),
                                         argumentSlotOffset)
}
case class MorselArgumentStateBufferOutputState(override val workIdentity: WorkIdentity,
                                                sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]],
                                                argumentSlotOffset: Int) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {
    val viewsPerArgument = ArgumentStateMap.map(argumentSlotOffset, outputMorsel, morselView => morselView)
    MorselArgumentStateBufferPreparedOutput(sink, viewsPerArgument)
  }
}
case class MorselArgumentStateBufferPreparedOutput(sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]],
                                                   data: IndexedSeq[PerArgument[MorselExecutionContext]]) extends PreparedOutput {
  override def produce(): Unit = sink.put(data)
}
