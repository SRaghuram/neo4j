package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.zombie.{ExecutionState, Task}

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
}

trait PreparedOutput {
  def produce(): Unit
}

// NO OUTPUT

case object NoOutput extends OutputOperator with OutputOperatorState with PreparedOutput {
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

case class MorselBufferOutput(bufferId: BufferId) extends OutputOperator {
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



