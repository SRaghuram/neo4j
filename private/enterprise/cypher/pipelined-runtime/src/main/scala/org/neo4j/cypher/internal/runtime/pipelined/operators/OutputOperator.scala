/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.Task
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeOperator.updateProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityImpl
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
  def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState
}

trait OutputOperatorState extends HasWorkIdentity {

  def trackTime: Boolean
  def prepareOutputWithProfile(output: Morsel,
                               state: PipelinedQueryState,
                               resources: QueryResources,
                               queryProfiler: QueryProfiler): PreparedOutput = {

    val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId.x, trackTime)
    resources.setKernelTracer(operatorExecutionEvent)
    if (state.doProfile) {
      resources.profileInformation = new InterpretedProfileInformation
    }
    try {
      val preparedOutput = prepareOutput(output, state, resources, operatorExecutionEvent)
      if (state.doProfile) {
        updateProfileEvent(operatorExecutionEvent, resources.profileInformation)
      }
      preparedOutput
    } finally {
      resources.setKernelTracer(null)
      if (operatorExecutionEvent != null) {
        operatorExecutionEvent.close()
      }
    }
  }

  protected def prepareOutput(outputMorsel: Morsel,
                              state: PipelinedQueryState,
                              resources: QueryResources,
                              operatorExecutionEvent: OperatorProfileEvent): PreparedOutput

  def canContinueOutput: Boolean = false
}

trait PreparedOutput extends AutoCloseable {
  def produce(): Unit
  override def close(): Unit = {}
}

// NO OUTPUT

case object NoOutputOperator extends OutputOperator with OutputOperatorState with PreparedOutput {
  override def outputBuffer: Option[BufferId] = None
  override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = this
  override def prepareOutput(outputMorsel: Morsel,
                             state: PipelinedQueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = this

  override def produce(): Unit = ()
  override def workIdentity: WorkIdentity = WorkIdentityImpl(Id.INVALID_ID, "Perform no output")
  override def trackTime: Boolean = true
}

// PIPELINED BUFFER OUTPUT

// we need the the id of the head plan of the next pipeline because that is where er attribute time spent
// during prepare output in profiling.
case class MorselBufferOutputOperator(bufferId: BufferId, nextPipelineHeadPlanId: Id, nextPipelineCanTrackTime: Boolean) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(nextPipelineHeadPlanId, s"Output morsel to $bufferId")
  override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState =
    //if nextPipelineCanTrackTime is false we shouldn't attribute time to nextPipelineHeadPlanId
    MorselBufferOutputState(workIdentity, nextPipelineCanTrackTime, bufferId, executionState)
}
case class MorselBufferOutputState(override val workIdentity: WorkIdentity,
                                   override val trackTime: Boolean,
                                   bufferId: BufferId,
                                   executionState: ExecutionState) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: Morsel,
                             state: PipelinedQueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput =
    MorselBufferPreparedOutput(bufferId, executionState, outputMorsel)
}
case class MorselBufferPreparedOutput(bufferId: BufferId,
                                      executionState: ExecutionState,
                                      outputMorsel: Morsel) extends PreparedOutput {
  override def produce(): Unit =
    executionState.putMorsel(bufferId, outputMorsel)
}

// PIPELINED ARGUMENT STATE BUFFER OUTPUT

// we need the the id of the head plan of the next pipeline because that is where er attribute time spent
// during prepare output in profiling.
case class MorselArgumentStateBufferOutputOperator(bufferId: BufferId, argumentSlotOffset: Int, nextPipelineHeadPlanId: Id, nextPipelineCanTrackTime: Boolean) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(nextPipelineHeadPlanId, s"Output morsel grouped by argumentSlot $argumentSlotOffset to $bufferId")
  override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState =
  //if nextPipelineCanTrackTime is false we shouldn't attribute time to nextPipelineHeadPlanId
    MorselArgumentStateBufferOutputState(workIdentity,
      executionState.getSink[IndexedSeq[PerArgument[Morsel]]](bufferId),
      argumentSlotOffset, nextPipelineCanTrackTime)
}
case class MorselArgumentStateBufferOutputState(override val workIdentity: WorkIdentity,
                                                sink: Sink[IndexedSeq[PerArgument[Morsel]]],
                                                argumentSlotOffset: Int,
                                                trackTime: Boolean) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: Morsel,
                             state: PipelinedQueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {
    val viewsPerArgument = ArgumentStateMap.map(argumentSlotOffset, outputMorsel, morselView => morselView)
    MorselArgumentStateBufferPreparedOutput(sink, viewsPerArgument)
  }
}
case class MorselArgumentStateBufferPreparedOutput(sink: Sink[IndexedSeq[PerArgument[Morsel]]],
                                                   data: IndexedSeq[PerArgument[Morsel]]) extends PreparedOutput {
  override def produce(): Unit = sink.put(data)
}
