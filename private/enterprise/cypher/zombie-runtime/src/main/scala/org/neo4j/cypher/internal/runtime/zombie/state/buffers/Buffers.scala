/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state.buffers

import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffers.{AccumulatingBuffer, SinkByOrigin}
import org.neo4j.cypher.internal.runtime.zombie.state.{ArgumentStateMap, QueryCompletionTracker, StateFactory}

/**
  * Container for all buffers of the execution state.
  *
  * This class takes responsibility of organizing the buffers and returning
  * the buffers casted to a more specific type, depending on which retrieval method was called.
  * The reason to do it this way is to have one location only where we make these assumptions
  * and have method names that clearly communicate what type of buffer we expect.
  */
class Buffers(bufferDefinitions: IndexedSeq[BufferDefinition],
              tracker: QueryCompletionTracker,
              argumentStateMaps: ArgumentStateMaps,
              stateFactory: StateFactory) {

  private val buffers: Array[SinkByOrigin] = new Array[SinkByOrigin](bufferDefinitions.length)

  // Static initializer code
    /**
      * This finds the first buffer downstream of the one at `initialIndex`, which accumulates
      * results. This is needed to set the reducer correctly, which in turn manages reference counting
      * on the accumulating buffer.
      *
      * @param initialIndex the index of the buffer to start the search from.
      * @param argumentStateMapId    the reducer for which we try to find the corresponding buffer.
      * @return the reducer
      */
    private def findRHSAccumulatingStateBuffer(initialIndex: Int, argumentStateMapId: ArgumentStateMapId): AccumulatingBuffer = {
      var j = initialIndex + 1
      while (j < buffers.length) {
        buffers(j) match {
          case x: LHSAccumulatingRHSStreamingBuffer[_] if x.lhsArgumentStateMapId == argumentStateMapId =>
            return x.LHSSink
          case x: LHSAccumulatingRHSStreamingBuffer[_] if x.rhsArgumentStateMapId == argumentStateMapId =>
            return x.RHSSink
          case x: MorselArgumentStateBuffer[_] if x.argumentStateMapId == argumentStateMapId =>
            return x
          case _ =>
        }
        j += 1
      }
      throw new IllegalStateException(s"Could not find downstream argumentStateBuffer with id $argumentStateMapId")
    }

    /* Go through the buffer definitions in reverse order and initialize an actual buffer for each definition.
     * Reverse order is needed, since we need to set reducers on upstream buffers, for which the downstream buffer
     * has to exist already.
     */
    private var i = buffers.length - 1
    while (i >= 0) {
      val bufferDefinition = bufferDefinitions(i)
      val reducers = bufferDefinition.reducers.map(asmId => findRHSAccumulatingStateBuffer(i, asmId))
      val workCancellers = bufferDefinition.workCancellers.map(_.id)
      buffers(i) =
        bufferDefinition match {
          case x: ApplyBufferDefinition =>
            val reducerOnRHSDefs = x.reducersOnRHS
            val argumentStatesToInitiate = workCancellers
            // argumentReducersForThis in reverse order, since upstream reducers possibly
            // need to increment counts on their downstreams, which have to be initialized
            // first in order to do that
            val reducersOnRHS = reducerOnRHSDefs.map(argStateDef => findRHSAccumulatingStateBuffer(i, argStateDef.id)).reverse
            new MorselApplyBuffer(argumentStatesToInitiate,
                                  reducersOnRHS,
                                  argumentReducersAfterThisApply = reducers,
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
            new LHSAccumulatingRHSStreamingBuffer(
              tracker,
              reducers,
              argumentStateMaps,
              x.lhsargumentStateMapId,
              x.rhsargumentStateMapId,
              x.lhsPipelineId,
              x.rhsPipelineId,
              stateFactory)

          case _: BufferDefinition =>
            new MorselBuffer(tracker, reducers, workCancellers, argumentStateMaps, stateFactory.newBuffer())
        }
      i -= 1
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
  def sink(fromPipeline: PipelineId, bufferId: BufferId): Sink[MorselExecutionContext] =
    buffers(bufferId.x).sinkFor(fromPipeline)

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
    * Get the buffer with the given id casted as a [[MorselArgumentStateBuffer]].
    */
  def argumentStateBuffer[ACC <: MorselAccumulator](bufferId: BufferId): MorselArgumentStateBuffer[ACC] =
    buffers(bufferId.x).asInstanceOf[MorselArgumentStateBuffer[ACC]]

  /**
    * Get the buffer with the given id casted as a [[LHSAccumulatingRHSStreamingBuffer]].
    */
  def lhsAccumulatingRhsStreamingBuffer[LHS_ACC <: MorselAccumulator](bufferId: BufferId): LHSAccumulatingRHSStreamingBuffer[LHS_ACC] =
    buffers(bufferId.x).asInstanceOf[LHSAccumulatingRHSStreamingBuffer[LHS_ACC]]
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
    def sinkFor(fromPipeline: PipelineId): Sink[MorselExecutionContext]
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
    def initiate(argumentRowId: Long): Unit

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
}
