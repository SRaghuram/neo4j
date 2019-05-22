/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.morsel.{ExecutionState, Task}
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkIdentity, WorkIdentityImpl}

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

trait OutputOperatorState {
  def prepareOutput(outputMorsel: MorselExecutionContext,
                    context: QueryContext,
                    state: QueryState,
                    resources: QueryResources): PreparedOutput

  def canContinue: Boolean = false
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
                             resources: QueryResources): PreparedOutput = this

  override def produce(): Unit = ()
  override def workIdentity: WorkIdentity = WorkIdentityImpl(-1, "Perform no output")
}

// MORSEL BUFFER OUTPUT

case class MorselBufferOutputOperator(bufferId: BufferId) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(bufferId.x, s"Output morsel to $bufferId")
  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState =
    MorselBufferOutputState(bufferId, executionState, pipelineId)
}
case class MorselBufferOutputState(bufferId: BufferId,
                                   executionState: ExecutionState,
                                   pipelineId: PipelineId) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources): PreparedOutput =
    MorselBufferPreparedOutput(bufferId, executionState, pipelineId, outputMorsel)
}
case class MorselBufferPreparedOutput(bufferId: BufferId,
                                      executionState: ExecutionState,
                                      pipelineId: PipelineId,
                                      outputMorsel: MorselExecutionContext) extends PreparedOutput {
  override def produce(): Unit =
    executionState.putMorsel(pipelineId, bufferId, outputMorsel)
}

// MORSEL ARGUMENT STATE BUFFER OUTPUT

case class MorselArgumentStateBufferOutputOperator(bufferId: BufferId, argumentSlotOffset: Int) extends OutputOperator {
  override def outputBuffer: Option[BufferId] = Some(bufferId)
  override val workIdentity: WorkIdentity = WorkIdentityImpl(bufferId.x, s"Output morsel grouped by argumentRowId $argumentSlotOffset to $bufferId")
  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState =
    MorselArgumentStateBufferOutputState(executionState.getSink[IndexedSeq[PerArgument[MorselExecutionContext]]](pipelineId, bufferId), argumentSlotOffset)
}
case class MorselArgumentStateBufferOutputState(sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]],
                                                argumentSlotOffset: Int) extends OutputOperatorState {
  override def prepareOutput(outputMorsel: MorselExecutionContext,
                             context: QueryContext,
                             state: QueryState,
                             resources: QueryResources): PreparedOutput = {
    val viewsPerArgument = ArgumentStateMap.map(argumentSlotOffset, outputMorsel, morselView => morselView)
    MorselArgumentStateBufferPreparedOutput(sink, viewsPerArgument)
  }
}
case class MorselArgumentStateBufferPreparedOutput(sink: Sink[IndexedSeq[PerArgument[MorselExecutionContext]]],
                                                   data: IndexedSeq[PerArgument[MorselExecutionContext]]) extends PreparedOutput {
  override def produce(): Unit = sink.put(data)
}



