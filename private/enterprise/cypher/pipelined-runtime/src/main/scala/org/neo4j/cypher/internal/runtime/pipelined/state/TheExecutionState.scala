/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined._
import org.neo4j.cypher.internal.runtime.pipelined.execution._
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.{Buffer, Buffers, Sink}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.util.Preconditions

/**
  * Implementation of [[ExecutionState]].
  */
class TheExecutionState(executionGraphDefinition: ExecutionGraphDefinition,
                        pipelines: Seq[ExecutablePipeline],
                        stateFactory: StateFactory,
                        workerWaker: WorkerWaker,
                        queryContext: QueryContext,
                        queryState: QueryState,
                        initializationResources: QueryResources,
                        tracker: QueryCompletionTracker) extends ExecutionState {

  verifyThatIdsAndOffsetsMatch()

  // Add assertion for query completion
  tracker.addCompletionAssertion(() => this.assertEmpty())

  // State

  // Execution state building works as a single sweep pass over the execution DAG, starting
  // from the produce results pipeline and moving towards the first pipeline and initial buffer.
  //
  // Each pipeline and buffer state is built as we traverse the DAG, meaning that pipelines
  // can acquire direct references to their sinks, and buffers can acquire references to
  // downstream buffers that they have to reference count for, and to the argument state maps
  // of any reducing operators.

  private val queryStatus = new QueryStatus
  private val argumentStateMapHolder = new Array[ArgumentStateMap[_ <: ArgumentState]](executionGraphDefinition.argumentStateMaps.size)
  override val argumentStateMaps: ArgumentStateMap.ArgumentStateMaps = id => argumentStateMapHolder(id.x)
  private val buffers: Buffers = new Buffers(executionGraphDefinition.buffers.size,
                                             tracker,
                                             argumentStateMaps,
                                             stateFactory)


  // This can hold a CleanUpTask if the query was cancelled. It will get scheduled before anything else.
  private val cleanUpTaskHolder = stateFactory.newSingletonBuffer[CleanUpTask]()

  override val pipelineStates: Array[PipelineState] = {
    val states = new Array[PipelineState](pipelines.length)
    var i = states.length - 1
    while (i >= 0) {
      val pipeline = pipelines(i)
      pipeline.outputOperator.outputBuffer.foreach(bufferId =>
                                                   buffers.constructBuffer(executionGraphDefinition.buffers(bufferId.x)))
      states(i) = pipeline.createState(this, queryContext, queryState, initializationResources, stateFactory)
      buffers.constructBuffer(pipeline.inputBuffer)
      i -= 1
    }
    // We don't reach the first apply buffer because it is not the output buffer of any pipeline, and also
    // not the input buffer, because all apply buffers are consumed through delegates.
    buffers.constructBuffer(executionGraphDefinition.buffers.head)
    states
  }

  private val pipelineLocks: Array[Lock] = {
    val arr = new Array[Lock](pipelines.length)
    var i = 0
    while (i < arr.length) {
      val pipeline = pipelines(i)
      arr(i) = stateFactory.newLock(s"Pipeline[${pipeline.id.x}]")
      i += 1
    }
    arr
  }

  private val continuations: Array[Buffer[PipelineTask]] = {
    val arr = new Array[Buffer[PipelineTask]](pipelines.length)
    var i = 0
    while (i < arr.length) {
      val pipeline = pipelines(i)
      arr(i) =
        if (pipeline.serial) stateFactory.newSingletonBuffer[PipelineTask]()
        else stateFactory.newBuffer[PipelineTask]()
      i += 1
    }
    arr
  }

  override def initializeState(): Unit = {
    // Assumption: Buffer with ID 0 is the initial buffer
    putMorsel(NO_PIPELINE, BufferId(0), MorselExecutionContext.createInitialRow())
  }

  // Methods

  override def getSink[T <: AnyRef](fromPipeline: PipelineId,
                                    bufferId: BufferId): Sink[T] = {
    new AlarmSink(buffers.sink[T](fromPipeline, bufferId), workerWaker, queryStatus)
  }

  override def putMorsel(fromPipeline: PipelineId,
                         bufferId: BufferId,
                         output: MorselExecutionContext): Unit = {
    if (!queryStatus.cancelled) {
      buffers.sink[MorselExecutionContext](fromPipeline, bufferId).put(output)
      workerWaker.wakeOne()
    } else {
      DebugSupport.ERROR_HANDLING.log("Dropped morsel %s because of query cancellation", output)
    }
  }

  override def takeMorsel(bufferId: BufferId): MorselParallelizer = {
    buffers.morselBuffer(bufferId).take()
  }

  override def takeAccumulator[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](bufferId: BufferId): ACC = {
    buffers.source[ACC](bufferId).take()
  }

  override def takeAccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](bufferId: BufferId): AccumulatorAndMorsel[DATA, ACC] = {
    buffers.source[AccumulatorAndMorsel[DATA, ACC]](bufferId).take()
  }

  override def takeData[DATA <: AnyRef](bufferId: BufferId): DATA = {
    buffers.source[DATA](bufferId).take()
  }

  override def closeWorkUnit(pipeline: ExecutablePipeline): Unit = {
    if (pipeline.serial) {
      pipelineLocks(pipeline.id.x).unlock()
    }
  }

  override def closeMorselTask(pipeline: ExecutablePipeline, inputMorsel: MorselExecutionContext): Unit = {
    closeWorkUnit(pipeline)
    buffers.morselBuffer(pipeline.inputBuffer.id).close(inputMorsel)
  }

  override def closeData[DATA <: AnyRef](pipeline: ExecutablePipeline, data: DATA): Unit = {
    closeWorkUnit(pipeline)
    buffers.closingSource(pipeline.inputBuffer.id).close(data)
  }

  override def closeAccumulatorTask(pipeline: ExecutablePipeline, accumulator: MorselAccumulator[_]): Unit = {
    closeWorkUnit(pipeline)
    buffers.argumentStateBuffer(pipeline.inputBuffer.id).close(accumulator)
  }

  override def closeMorselAndAccumulatorTask(pipeline: ExecutablePipeline,
                                             inputMorsel: MorselExecutionContext,
                                             accumulator: MorselAccumulator[_]): Unit = {
    closeWorkUnit(pipeline)
    val buffer = buffers.lhsAccumulatingRhsStreamingBuffer(pipeline.inputBuffer.id)
    buffer.close(accumulator, inputMorsel)
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                        inputMorsel: MorselExecutionContext): Boolean = {
    buffers.morselBuffer(pipeline.inputBuffer.id).filterCancelledArguments(inputMorsel)
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                        accumulator: MorselAccumulator[_]): Boolean = {
    buffers.argumentStateBuffer(pipeline.inputBuffer.id).filterCancelledArguments(accumulator)
  }

  override def filterCancelledArguments(pipeline: ExecutablePipeline,
                                        inputMorsel: MorselExecutionContext,
                                        accumulator: MorselAccumulator[_]): Boolean = {
    val buffer = buffers.lhsAccumulatingRhsStreamingBuffer(pipeline.inputBuffer.id)
    buffer.filterCancelledArguments(accumulator, inputMorsel)
  }

  override def putContinuation(task: PipelineTask, wakeUp: Boolean, resources: QueryResources): Unit = {
    if (queryStatus.cancelled) {
      DebugSupport.ERROR_HANDLING.log("[putContinuation] Closing %s because of query cancellation", task)
      task.close(resources)
    } else {
      DebugSupport.BUFFERS.log("[putContinuation]   %s <- %s", this, task)
      // Put the continuation before unlocking (closeWorkUnit)
      // so that in serial pipelines we can guarantee that the continuation
      // is the next thing which is picked up
      continuations(task.pipelineState.pipeline.id.x).put(task)
      if (wakeUp && !task.pipelineState.pipeline.serial) {
        // We only wake up other Threads if this pipeline is not serial.
        // Otherwise they will all race to get this continuation while
        // this Thread can just as well continue on its own.
        workerWaker.wakeOne()
      }
      closeWorkUnit(task.pipelineState.pipeline)
    }
  }

  override def canPut(pipeline: ExecutablePipeline): Boolean = {
    pipeline.outputOperator.outputBuffer match {
      case None => true
      case Some(bufferId) => buffers.sink(pipeline.id, bufferId).canPut
    }
  }

  override def takeContinuation(pipeline: ExecutablePipeline): PipelineTask = {
    continuations(pipeline.id.x).take()
  }

  override def tryLock(pipeline: ExecutablePipeline): Boolean = pipelineLocks(pipeline.id.x).tryLock()

  override def unlock(pipeline: ExecutablePipeline): Unit = pipelineLocks(pipeline.id.x).unlock()

  override def canContinueOrTake(pipeline: ExecutablePipeline): Boolean = {
    val hasWork = continuations(pipeline.id.x).hasData || buffers.hasData(pipeline.inputBuffer.id)
    hasWork && queryState.flowControl.hasDemand
  }

  override final def createArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                                factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    val argumentSlotOffset = executionGraphDefinition.argumentStateMaps(argumentStateMapId.x).argumentSlotOffset
    val asm = stateFactory.newArgumentStateMap(argumentStateMapId, argumentSlotOffset, factory)
    argumentStateMapHolder(argumentStateMapId.x) = asm
    asm
  }

  /**
    * Mark this query as failed, and close any outstanding work.
    *
    * @param throwable the observed exception
    */
  override def failQuery(throwable: Throwable,
                         resources: QueryResources,
                         failedPipeline: ExecutablePipeline): Unit = {

    DebugSupport.ERROR_HANDLING.log("Starting ExecutionState.failQuery, because of %s", throwable)
    tracker.error(throwable)
    closeOutstandingWork(resources, failedPipeline)
  }

  /**
    * Cancel this query, and close any outstanding work.
    */
  override def cancelQuery(resources: QueryResources): Unit = {
    closeOutstandingWork(resources, failedPipeline = null)
  }

  override def scheduleCancelQuery(): Unit = {
    cleanUpTaskHolder.tryPut(new CleanUpTask(this))
  }

  override def cleanUpTask(): CleanUpTask = {
    cleanUpTaskHolder.take()
  }

  /**
    * To achieve a clean shut-down, we
    *
    *  - close any new continuations being put into the execution state
    *  - drop any new data being put into the execution state
    *  - take and close all continuations and data we find in the execution state
    *
    *  This method is intended to be called only once, but written in a way that allows
    *  multiple calls to it that behave idempotent. This is needed if several threads encounter
    *  Exceptions simultaneously.
    *
    * @param resources      resources where to hand-back any open cursors
    * @param failedPipeline pipeline that was executing while the failure occurred, or `null` if
    *                       the failure happened outside of pipeline execution
    */
  private def closeOutstandingWork(resources: QueryResources, failedPipeline: ExecutablePipeline): Unit = {
    queryStatus.cancelled = true

    buffers.clearAll()

    for (pipeline <- pipelines) {
      val continuationBuffer = continuations(pipeline.id.x)
      DebugSupport.ERROR_HANDLING.log("Clearing continuation buffer %s", continuationBuffer)
      if (!pipeline.serial || pipeline == failedPipeline || tryLock(pipeline)) {
        var c = continuationBuffer.take()
        while (c != null) {
          c.close(resources)
          c = continuationBuffer.take()
        }
      }
    }
  }

  override def hasEnded: Boolean = tracker.hasEnded

  override def prettyString(pipeline: ExecutablePipeline): String = {
    s"""continuations: ${continuations(pipeline.id.x)}
       |""".stripMargin
  }

  /**
    * Assert that all buffers, continuations and argument state maps are empty.
    */
  private def assertEmpty(): Unit = {
    buffers.assertAllEmpty()
    var i = 0
    while (i < continuations.length) {
      val continuation = continuations(i)
      if (continuation.hasData) {
        throw new RuntimeResourceLeakException(s"Continuation buffer $continuation is not empty after query completion.")
      }
      i += 1
    }
  }

  override def toString: String = "TheExecutionState"

  private def verifyThatIdsAndOffsetsMatch(): Unit = {
    if (AssertionRunner.isAssertionsEnabled) {
      var i = 0
      while (i < pipelines.length) {
        Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")
        i += 1
      }

      i = 0
      while (i < executionGraphDefinition.buffers.size) {
        Preconditions.checkState(i == executionGraphDefinition.buffers(i).id.x, "Buffer definition id does not match offset!")
        i += 1
      }
    }
  }
}

class QueryStatus {
  @volatile var cancelled = false
}
