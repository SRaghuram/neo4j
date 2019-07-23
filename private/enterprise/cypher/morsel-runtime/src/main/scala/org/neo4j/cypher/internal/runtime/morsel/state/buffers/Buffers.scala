/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.{AccumulatingBuffer, DataHolder, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, QueryCompletionTracker, StateFactory}

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
              stateFactory: StateFactory) {

  private val buffers: Array[SinkByOrigin] = new Array[SinkByOrigin](numBuffers)

  // Constructor code

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
        case x: LHSAccumulatingRHSStreamingBuffer[_, _] if x.lhsArgumentStateMapId == argumentStateMapId =>
          return x.LHSSink
        case x: LHSAccumulatingRHSStreamingBuffer[_, _] if x.rhsArgumentStateMapId == argumentStateMapId =>
          return x.RHSSink
        case x: MorselArgumentStateBuffer[_, _] if x.argumentStateMapId == argumentStateMapId =>
          return x
        case x: OptionalMorselBuffer if x.argumentStateMapId == argumentStateMapId =>
          return x
        case _ =>
      }
      j += 1
    }
    throw new IllegalStateException(s"Could not find downstream argumentStateBuffer with id $argumentStateMapId")
  }

  private[state] def constructBuffer(bufferDefinition: BufferDefinition): Unit = {
    val i = bufferDefinition.id.x

    // Since apply buffers delegate, the output buffer of the producing pipeline and the input
    // buffer of the consuming pipeline(s) will not be logically the same. Therefore we explicitly
    // construct both the output and input buffers of all pipelines, but since they are still mostly
    // the same, we need to idempotently ignore the construct call if the buffer is already constructed.
    if (buffers(i) != null)
      return

    val reducers = bufferDefinition.downstreamStates.collect {
      case DownstreamReduce(asmId) => findRHSAccumulatingStateBuffer(i, asmId)
    }
    val workCancellerIDs = bufferDefinition.downstreamStates.collect{
      case DownstreamWorkCanceller(asmId) => asmId
    }
    val simpleDownstreamStateIDs = bufferDefinition.downstreamStates.collect {
      case DownstreamState(asmId) => asmId
    }

    buffers(i) =
      bufferDefinition match {
        case x: ApplyBufferDefinition =>
          val reducerOnRHSDefs = x.reducersOnRHS
          val argumentStatesToInitiate = workCancellerIDs ++ simpleDownstreamStateIDs
          // argumentReducersForThis in reverse order, since upstream reducers possibly
          // need to increment counts on their downstreams, which have to be initialized
          // first in order to do that
          val reducersOnRHS = reducerOnRHSDefs.map(argStateDef => findRHSAccumulatingStateBuffer(i,
                                                                                                 argStateDef.id)).reverse
          new MorselApplyBuffer(x.id,
                                argumentStatesToInitiate,
                                reducersOnRHS,
                                argumentReducersOnTopOfThisApply = reducers,
                                argumentStateMaps,
                                x.argumentSlotOffset,
                                stateFactory.newIdAllocator(),
                                x.delegates.map(bufferId => morselBuffer(bufferId)))

        case x: ArgumentStateBufferDefinition =>
          new MorselArgumentStateBuffer(tracker,
                                        reducers,
                                        argumentStateMaps,
                                        x.argumentStateMapId)

        case x: LHSAccumulatingRHSStreamingBufferDefinition =>
          new LHSAccumulatingRHSStreamingBuffer(tracker,
                                                reducers,
                                                argumentStateMaps,
                                                x.lhsArgumentStateMapId,
                                                x.rhsArgumentStateMapId,
                                                x.lhsPipelineId,
                                                x.rhsPipelineId,
                                                stateFactory)

        case x: OptionalMorselBufferDefinition =>
          new OptionalMorselBuffer(x.id,
                                   tracker,
                                   reducers,
                                   argumentStateMaps,
                                   x.argumentStateMapId)

        case x: BufferDefinition =>
          new MorselBuffer(x.id, tracker, reducers, workCancellerIDs, argumentStateMaps, stateFactory.newBuffer[MorselExecutionContext]())
      }
  }

  // Public methods

  /**
    * Get the [[Sink]] for the given buffer id. This is usually the buffer itself,
    * but buffers that accept input from two sides will differentiate depending on the given
    * PipelineId.
    *
    * @param fromPipeline the pipeline that wants to obtain the sink, to put data into it.
    * @param bufferId     the buffer
    * @return the Sink.
    */
  def sink[T <: AnyRef](fromPipeline: PipelineId, bufferId: BufferId): Sink[T] =
    buffers(bufferId.x).sinkFor[T](fromPipeline)

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
    * Get the buffer with the given id casted as a [[LHSAccumulatingRHSStreamingBuffer]].
    */
  def lhsAccumulatingRhsStreamingBuffer(bufferId: BufferId): LHSAccumulatingRHSStreamingBuffer[_, _] =
    buffers(bufferId.x).asInstanceOf[LHSAccumulatingRHSStreamingBuffer[_, _]]

  /**
    * Clear all data from all buffers.
    */
  def clearAll(): Unit = {
    var i = 0
    while (i < buffers.length) {
      val buffer = buffers(i)
      buffer match {
        case dataHolder: DataHolder =>
          DebugSupport.logErrorHandling(s"Clearing $dataHolder")
          dataHolder.clearAll()
        case x => // nothing to do here
          DebugSupport.logErrorHandling(s"Not clearing $x")
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
}

/**
  * I set of interfaces that certain types of buffers will implement.
  */
object Buffers {

  /**
    * Trait to implement by all buffers. They must be able to provide a sink, depending
    * on where the data to insert comes from.
    */
  trait SinkByOrigin {
    /**
      * Get the [[Sink]] for the given data origin. This is usually the buffer itself,
      * but buffers that accept input from two sides will differentiate depending on the given
      * PipelineId.
      *
      * @param fromPipeline the pipeline that wants to obtain the sink, to put data into it.
      * @return the Sink.
      */
    def sinkFor[T <: AnyRef](fromPipeline: PipelineId): Sink[T]
  }

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
      * @return the offset of the argument slots for the used [[ArgumentStateMap]]
      */
    def argumentSlotOffset: Int

    /**
      * Initiate the accumulator relevant to the given argument ID.
      * If the accumulator is already initiated, this will increment the count.
      * Will be called when upstream apply buffers receive new morsels.
      */
    def initiate(argumentRowId: Long, argumentMorsel: MorselExecutionContext): Unit

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
  case class AccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](acc: ACC, morsel: MorselExecutionContext)
}
