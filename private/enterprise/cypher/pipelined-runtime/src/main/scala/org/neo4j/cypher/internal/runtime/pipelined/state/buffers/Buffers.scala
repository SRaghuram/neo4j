/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.physicalplanning.AntiType
import org.neo4j.cypher.internal.physicalplanning.ApplyBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.ArgumentStreamBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStreamType
import org.neo4j.cypher.internal.physicalplanning.AttachBufferVariant
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ConditionalBufferVariant
import org.neo4j.cypher.internal.physicalplanning.Initialization
import org.neo4j.cypher.internal.physicalplanning.LHSAccumulatingBufferVariant
import org.neo4j.cypher.internal.physicalplanning.LHSAccumulatingRHSStreamingBufferVariant
import org.neo4j.cypher.internal.physicalplanning.RHSStreamingBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.physicalplanning.RegularBufferVariant
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.DataHolder

/**
 * Container for all buffers of the execution state.
 *
 * This class takes responsibility of organizing the buffers and returning
 * the buffers casted to a more specific type, depending on which retrieval method was called.
 * The reason to do it this way is to have one location only where we make these assumptions
 * and have method names that clearly communicate what type of buffer we expect.
 */
class Buffers(numBuffers: Int,
              tracker: QueryCompletionTracker,
              argumentStateMaps: ArgumentStateMaps,
              stateFactory: StateFactory,
              morselSize: Int) {

  private val buffers: Array[Any] = new Array[Any](numBuffers)

  // Constructor code

  override def toString: String = s"${getClass.getSimpleName}(buffers:${buffers.mkString(",")})"

  private def findRHSAccumulatingStateBuffers(initialIndex: Int,
                                              argumentStateMapIds: ReadOnlyArray[ArgumentStateMapId]
                                             ): ReadOnlyArray[AccumulatingBuffer] = {
    var j = 0
    val reducers = new Array[AccumulatingBuffer](argumentStateMapIds.length)
    while (j < argumentStateMapIds.length) {
      val asmId = argumentStateMapIds(j)
      val accumulatingBuffer = findRHSAccumulatingStateBuffer(initialIndex, asmId)
      reducers(j) = accumulatingBuffer
      j += 1
    }
    new ReadOnlyArray(reducers)
  }

  private def findRHSAccumulatingStateBuffersWithInitialization(initialIndex: Int,
                                                                argumentStateMapInitializations: ReadOnlyArray[Initialization[ArgumentStateMapId]]
                                                               ): ReadOnlyArray[Initialization[AccumulatingBuffer]] = {
    var j = 0
    val reducers = new Array[Initialization[AccumulatingBuffer]](argumentStateMapInitializations.length)
    while (j < argumentStateMapInitializations.length) {
      val Initialization(asmId, initialCount) = argumentStateMapInitializations(j)
      val accumulatingBuffer = findRHSAccumulatingStateBuffer(initialIndex, asmId)
      reducers(j) = Initialization(accumulatingBuffer, initialCount)
      j += 1
    }
    new ReadOnlyArray(reducers)
  }

  /**
   * This finds the first buffer downstream of the one at `initialIndex`, which accumulates
   * results. This is needed to set the reducer correctly, which in turn manages reference counting
   * on the accumulating buffer.
   *
   * @param initialIndex       index of the buffer to start the search from.
   * @param argumentStateMapId reducer for which we try to find the corresponding buffer.
   * @return                   AccumulatingBuffer
   */
  private def findRHSAccumulatingStateBuffer(initialIndex: Int, argumentStateMapId: ArgumentStateMapId): AccumulatingBuffer = {
    var j = initialIndex + 1
    while (j < buffers.length) {
      buffers(j) match {
        case x: LHSAccumulatingSink[_, _] if x.lhsArgumentStateMapId == argumentStateMapId =>
          return x
        case x: RHSStreamingSink if x.rhsArgumentStateMapId == argumentStateMapId =>
          return x
        case x: MorselArgumentStateBuffer[_, _] if x.argumentStateMapId == argumentStateMapId =>
          return x
        case x: ArgumentStreamMorselBuffer if x.argumentStateMapId == argumentStateMapId =>
          return x
        case x: AntiMorselBuffer if x.argumentStateMapId == argumentStateMapId =>
          return x
        case _ =>
      }
      j += 1
    }
    throw new IllegalStateException(s"Could not find downstream argumentStateBuffer with id $argumentStateMapId")
  }

  private[state] def constructBuffer(bufferDefinition: BufferDefinition, queryState: PipelinedQueryState): Unit = {
    val i = bufferDefinition.id.x

    // Since apply buffers delegate, the output buffer of the producing pipeline and the input
    // buffer of the consuming pipeline(s) will not be logically the same. Therefore we explicitly
    // construct both the output and input buffers of all pipelines, but since they are still mostly
    // the same, we need to idempotently ignore the construct call if the buffer is already constructed.
    if (buffers(i) != null) {
      return
    }

    val reducers = findRHSAccumulatingStateBuffers(i, bufferDefinition.reducers)

    buffers(i) =
      bufferDefinition.variant match {
        case x: AttachBufferVariant =>
          /*
          Explicit construction of Apply Buffer is necessary here due to the combination of:
          (1) during Execution State initialization only the buffers that are pipeline outputs are initialized
          (2) when solving Cartesian Product, the delegating Apply Buffer used by Attach Buffer is not an output of any pipeline, so would never be initialized
           */
          constructBuffer(x.applyBuffer, queryState)
          new MorselAttachBuffer(bufferDefinition.id,
            applyBuffer(x.applyBuffer.id),
            x.outputSlots,
            x.argumentSlotOffset,
            x.argumentSize.nLongs,
            x.argumentSize.nReferences)

        case x: ApplyBufferVariant =>
          val workCancellers = bufferDefinition.workCancellers
          val downstreamStates = x.downstreamStatesOnRHS
          val reducersOnRHS = findRHSAccumulatingStateBuffersWithInitialization(i, x.reducersOnRHSReversed)
          new MorselApplyBuffer(bufferDefinition.id,
            workCancellers,
            downstreamStates,
            reducersOnRHS,
            argumentReducersOnTopOfThisApply = reducers,
            argumentStateMaps,
            x.argumentSlotOffset,
            stateFactory.newIdAllocator(),
            morselBuffers(x.delegates))

        case x: ArgumentStateBufferVariant =>
          new MorselArgumentStateBuffer(tracker,
            reducers,
            argumentStateMaps,
            x.argumentStateMapId)

        case x: LHSAccumulatingBufferVariant =>
          new LHSAccumulatingSink(x.lhsArgumentStateMapId,
                                  x.rhsArgumentStateMapId,
                                  reducers,
                                  argumentStateMaps)
        case x: RHSStreamingBufferVariant =>
          new RHSStreamingSink(x.lhsArgumentStateMapId,
                               x.rhsArgumentStateMapId,
                               reducers,
                               argumentStateMaps,
                               tracker)
        case x: LHSAccumulatingRHSStreamingBufferVariant =>
          /*
          Explicit construction of sinks is necessary due to the combination of:
          (1) pipelines (and their output buffers) are initialized in reverse order
          (2) when solving Cartesian Product, LHS Sink of Join Buffer is not an output of any pipeline, so would never be initialized
           */
          constructBuffer(x.lhsSink, queryState)
          constructBuffer(x.rhsSink, queryState)
          new LHSAccumulatingRHSStreamingSource(tracker,
                                                reducers,
                                                argumentStateMaps,
                                                x.lhsArgumentStateMapId,
                                                x.rhsArgumentStateMapId)

        case ArgumentStreamBufferVariant(argumentStateMapId, ArgumentStreamType) =>
          new ArgumentStreamMorselBuffer(bufferDefinition.id,
            tracker,
            reducers,
            argumentStateMaps,
            argumentStateMapId)

        case ArgumentStreamBufferVariant(argumentStateMapId, AntiType) =>
          new AntiMorselBuffer(bufferDefinition.id,
            tracker,
            reducers,
            argumentStateMaps,
            argumentStateMapId,
            morselSize)

        case ConditionalBufferVariant(onTrue, onFalse, expression) =>
          constructBuffer(onTrue, queryState)
          constructBuffer(onFalse, queryState)
          new ConditionalSink(expression, sink(onTrue.id), sink(onFalse.id), queryState)

        case RegularBufferVariant =>
          new MorselBuffer(bufferDefinition.id,
            tracker,
            reducers,
            bufferDefinition.workCancellerIds,
            argumentStateMaps,
            stateFactory.newBuffer[Morsel](bufferDefinition.memoryTrackingOperatorId))
      }
  }

  // Public methods

  /**
    * Get the [[Sink]] for the given buffer id.
   *
   * @param bufferId     the buffer
    * @return the Sink.
    */
  def sink[T <: AnyRef](bufferId: BufferId): Sink[T] =
    buffers(bufferId.x).asInstanceOf[Sink[T]]

  /**
   * Get the buffer with the given id casted as a [[Source]].
   */
  def source[S <: AnyRef](bufferId: BufferId): Source[S] =
    buffers(bufferId.x).asInstanceOf[Source[S]]

  /**
   * @return if the buffer with the given id has data.
   */
  def hasData(bufferId: BufferId): Boolean =
    buffers(bufferId.x).asInstanceOf[Source[_]].hasData

  /**
   * Get the buffer with the given id casted as a [[MorselBuffer]].
   */
  def morselBuffer(bufferId: BufferId): MorselBuffer =
    buffers(bufferId.x).asInstanceOf[MorselBuffer]

  /**
   * Get the buffer with the given id casted as a [[MorselApplyBuffer]].
   */
  def applyBuffer(bufferId: BufferId): MorselApplyBuffer =
    buffers(bufferId.x).asInstanceOf[MorselApplyBuffer]

  /**
   * Get the buffer with the given ud casted as a [[ClosingSource]].
   */
  def closingSource[S <: AnyRef](bufferId: BufferId): ClosingSource[S] =
    buffers(bufferId.x).asInstanceOf[ClosingSource[S]]

  /**
   * Get the buffer with the given id casted as a [[MorselArgumentStateBuffer]].
   */
  def argumentStateBuffer(bufferId: BufferId): MorselArgumentStateBuffer[_, _] =
    buffers(bufferId.x).asInstanceOf[MorselArgumentStateBuffer[_, _]]

  /**
   * Get the buffer with the given id casted as a [[LHSAccumulatingRHSStreamingSource]].
   */
  def lhsAccumulatingRhsStreamingBuffer(bufferId: BufferId): LHSAccumulatingRHSStreamingSource[_, _] =
    buffers(bufferId.x).asInstanceOf[LHSAccumulatingRHSStreamingSource[_, _]]

  /**
   * Clear all data from all buffers.
   */
  def clearAll(): Unit = {
    var i = 0
    while (i < buffers.length) {
      val buffer = buffers(i)
      buffer match {
        case dataHolder: DataHolder =>
          DebugSupport.ERROR_HANDLING.log("Clearing %s", dataHolder)
          dataHolder.clearAll()
        case x => // nothing to do here
          DebugSupport.ERROR_HANDLING.log("Not clearing %s", x)
      }
      i += 1
    }
  }

  /**
   * Assert that all buffers are empty
   */
  def assertAllEmpty() : Unit = {
    var i = 0
    while (i < buffers.length) {
      val buffer = buffers(i)
      buffer match {
        case s: Source[_] =>
          if(s.hasData) {
            throw new RuntimeResourceLeakException(s"Buffer $s is not empty after query completion.")
          }
        case _ =>
      }
      i += 1
    }
  }

  // Specialization to remove overheads from scala collection `map`
  private def morselBuffers(bufferIds: ReadOnlyArray[BufferId]): ReadOnlyArray[MorselBuffer] = {
    val result = new Array[MorselBuffer](bufferIds.length)
    var i = 0
    while (i < result.length) {
      result(i) = morselBuffer(bufferIds(i))
      i += 1
    }
    new ReadOnlyArray[MorselBuffer](result)
  }
}

/**
 * I set of interfaces that certain types of buffers will implement.
 */
object Buffers {

  /**
   * Since some buffers merely augment and pass on data (e.g. [[MorselApplyBuffer]]), this trait
   * distinguishes the buffers that actually hold onto data.
   */
  trait DataHolder {

    /**
     * Clear all data from this data holder. This includes decrementing the [[QueryCompletionTracker]]
     * to make sure that the query execution in cleanly closed.
     */
    def clearAll(): Unit
  }

  /**
   * A buffer that accumulates data in some sort. The upstream buffer
   * must be able to both initiate accumulators and decrement counts.
   */
  trait AccumulatingBuffer {

    /**
     * @return the offset of the argument slots for the used [[org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap]]
     */
    def argumentSlotOffset: Int

    /**
     * Initiate the accumulator relevant to the given argument ID.
     * Will be called when upstream apply buffers receive new morsels.
     *
     * @param initialCount the initial count for the argument row id
     */
    def initiate(argumentRowId: Long, argumentMorsel: MorselReadCursor, initialCount: Int): Unit

    /**
     * Increment counts for the accumulator relevant to the given argument ID.
     * Will be called when upstream regular buffers receive new morsels.
     */
    def increment(argumentRowId: Long): Unit

    /**
     * Decrement counts for the accumulator relevant to the given argument ID.
     * Will be called when upstream pipelines (both regular and apply) complete.
     */
    def decrement(argumentRowId: Long): Unit
  }

  /**
   * Output of lhsAccumulatingRhsStreamingBuffers.
   */
  case class AccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](acc: ACC, morsel: Morsel)
}
