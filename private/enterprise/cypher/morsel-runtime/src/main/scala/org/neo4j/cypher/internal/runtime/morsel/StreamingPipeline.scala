/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.scheduling.{HasWorkIdentity, Task, WorkIdentity}
import org.neo4j.cypher.internal.v4_0.util.InternalException

import scala.collection.mutable.ArrayBuffer

/**
  * A streaming pipeline.
  */
class StreamingPipeline(override val start: StreamingOperator,
                        override val slots: SlotConfiguration,
                        upstream: Option[Pipeline])
  extends AbstractStreamingPipeline(slots, upstream)

abstract class AbstractStreamingPipeline(slots: SlotConfiguration,
                                         override val upstream: Option[Pipeline]) extends Pipeline {
  def start: StreamingOperator

  def init(inputMorsel: MorselExecutionContext,
           context: QueryContext,
           state: QueryState,
           resources: QueryResources): IndexedSeq[PipelineTask] = {
    val streamTasks = start.init(context, state, inputMorsel, resources)
    // init next reduce
    val nextState = initDownstreamReduce(state)

    streamTasks.map(pipelineTask(_, context, nextState, EmptyPipelineArgument))
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext,
                            context: QueryContext,
                            state: QueryState,
                            resources: QueryResources,
                            pipelineArgument: PipelineArgument,
                            from: AbstractPipelineTask): IndexedSeq[Task[QueryResources]] = {
    val tasks = start.init(context, state, inputMorsel, resources)
    tasks.map(pipelineTask(_, context, state, pipelineArgument))
  }

  override val workIdentity: WorkIdentity = composeWorkIdentities(start, operators)
}

/**
  * A streaming pipeline that can compose multiple composable operators into one.
  * It needs a StreamingOperator as an initial composable operator, then subsequent
  * composable operators can be added during pipeline building.
  * When it is time to construct the final pipeline graph, the added operators will be composed
  * together and the resulting composition will be injected into the start operator.
  *
  * A prime example is the fallback slotted pipe operator where it can be used to build a longer pipe
  * from several consecutive placeholder pipe operators, but it could also be used for
  * generating code for the combination of multiple operators (loop fusion).
  */
class StreamingComposablePipeline[T](override val start: StreamingOperator with InitialComposableOperator[T],
                                     var slots: SlotConfiguration,
                                     upstream: Option[Pipeline]) extends AbstractStreamingPipeline(slots, upstream) {

  // Composable operators
  private val composableOperators: ArrayBuffer[ComposableOperator[_]] = new ArrayBuffer[ComposableOperator[_]]

  def addComposableOperator(operator: ComposableOperator[_], slots: SlotConfiguration): Unit = {
    composableOperators += operator
    // We need to end up with the slot configuration of the last operator added, thus we override it here for every added operator
    this.slots = slots
  }

  override def connectPipeline(downstream: Option[Pipeline], downstreamReduce: Option[ReducePipeline]): Unit = {
    composeOperators()
    super.connectPipeline(downstream, downstreamReduce)
  }

  private def composeOperators(): Unit = {
    val composed =
      composableOperators.foldLeft(start.initialSource) {
        case (acc, operator: ComposableOperator[T]) =>
          operator.compose(acc)

        case _ =>
          throw new InternalException("Unexpected operator")
      }
    start.setFinalComposition(composed)
  }
}

// This is used as a placeholder during the execution of PipelineBuilder.
// Consecutive instances of ComposableOperators will be folded into a preceeding
// single InitialComposableOperator at pipeline construction/connection time,
// (after all pipelines have been built).
trait ComposableOperator[T] extends HasWorkIdentity {
  def compose(source: T): T
}

trait InitialComposableOperator[T] {
  def initialSource: T
  def setFinalComposition(fin: T): Unit // Only allowed to be called once!
}

