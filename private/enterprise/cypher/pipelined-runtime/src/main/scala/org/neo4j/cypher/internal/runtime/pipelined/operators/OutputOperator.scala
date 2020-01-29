/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.BufferId
<<<<<<< HEAD
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.Task
=======
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.QueryContext
>>>>>>> da402acfd95... Don't attribute any time to fused operators
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
<<<<<<< HEAD
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityImpl
import org.neo4j.cypher.internal.util.attribution.Id
=======
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.Task
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityImpl
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
>>>>>>> da402acfd95... Don't attribute any time to fused operators

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
  def createState(executionState: ExecutionState): OutputOperatorState
}

trait OutputOperatorState extends HasWorkIdentity {

  def trackTime: Boolean
  def prepareOutputWithProfile(output: MorselExecutionContext,
                               context: QueryContext,
                               state: QueryState,
                               resources: QueryResources,
                               queryProfiler: QueryProfiler): PreparedOutput = {

    val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId.x, trackTime)
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
  override def createState(executionState: ExecutionState): OutputOperatorState = this
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = this

  override def produce(): Unit = ()
  override def workIdentity: WorkIdentity = WorkIdentityImpl(Id.INVALID_ID, "Perform no output")
  override def trackTime: Boolean = true
}

// PIPELINED BUFFER OUTPUT

// we need the the id of the head plan of the next pipeline because that is where er attribute time spent
// during prepare output in profiling.
case class MorselBufferOutputOperator(bufferId: BufferId, nextPipelineHeadPlanId: Id, nextPipelineFused: Boolean) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(nextPipelineHeadPlanId, s"Output morsel to $bufferId")
<<<<<<< HEAD
  override def createState(executionState: ExecutionState): OutputOperatorState =
    MorselBufferOutputState(workIdentity, bufferId, executionState)
=======
  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState =
    MorselBufferOutputState(workIdentity, !nextPipelineFused, bufferId, executionState, pipelineId)
>>>>>>> da402acfd95... Don't attribute any time to fused operators
}
case class MorselBufferOutputState(override val workIdentity: WorkIdentity,
                                   override val trackTime: Boolean,
                                   bufferId: BufferId,
                                   executionState: ExecutionState) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources,
                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput =
    MorselBufferPreparedOutput(bufferId, executionState, outputMorsel)
}
case class MorselBufferPreparedOutput(bufferId: BufferId,
                                      executionState: ExecutionState,
                                      outputMorsel: MorselExecutionContext) extends PreparedOutput {
  override def produce(): Unit =
    executionState.putMorsel(bufferId, outputMorsel)
}

// PIPELINED ARGUMENT STATE BUFFER OUTPUT

// we need the the id of the head plan of the next pipeline because that is where er attribute time spent
// during prepare output in profiling.
case class MorselArgumentStateBufferOutputOperator(bufferId: BufferId, argumentSlotOffset: Int, nextPipelineHeadPlanId: Id, nextPipelineFused: Boolean) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(nextPipelineHeadPlanId, s"Output morsel grouped by argumentSlot $argumentSlotOffset to $bufferId")
  override def createState(executionState: ExecutionState): OutputOperatorState =
    MorselArgumentStateBufferOutputState(workIdentity,
<<<<<<< HEAD
      executionState.getSink[IndexedSeq[PerArgument[MorselExecutionContext]]](bufferId),
      argumentSlotOffset)
=======
                                         executionState.getSink[IndexedSeq[PerArgument[MorselExecutionContext]]](pipelineId, bufferId),
                                         argumentSlotOffset,
                                         !nextPipelineFused)
>>>>>>> da402acfd95... Don't attribute any time to fused operators
}
case class MorselArgumentStateBufferOutputState(workIdentity: WorkIdentity,
                                                sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]],
                                                argumentSlotOffset: Int,
                                                trackTime: Boolean) extends OutputOperatorState {
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
