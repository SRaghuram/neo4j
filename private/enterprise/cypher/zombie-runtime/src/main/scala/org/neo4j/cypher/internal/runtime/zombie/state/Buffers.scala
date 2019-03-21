/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.{ApplyBufferDefinition, ArgumentStateBufferDefinition, BufferDefinition, BufferId}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Container for all buffers of the execution state.
  */
class Buffers(bufferDefinitions: IndexedSeq[BufferDefinition],
              tracker: QueryCompletionTracker,
              argumentStateMaps: ArgumentStateMaps,
              stateFactory: StateFactory) {

  private val buffers: Array[Sink[MorselExecutionContext]] = {
    val array = new Array[Sink[MorselExecutionContext]](bufferDefinitions.length)
    def findDownstreamArgumentStateBuffer(initialIndex: Int, reducePlanId: Id): MorselArgumentStateBuffer[_] = {
      var j = initialIndex + 1
      while (j < array.length) {
        array(j) match {
          case x: MorselArgumentStateBuffer[_] if x.reducePlanId == reducePlanId =>
            return x
          case _ =>
        }
        j += 1
      }
      throw new IllegalStateException(s"Could not find downstream argumentStateBuffer with id $reducePlanId")
    }

    for (i <- array.indices.reverse) {
      val bufferDefinition = bufferDefinitions(i)
      val reducers = bufferDefinition.reducers
      val workCancellers = bufferDefinition.workCancellers
      array(i) =
        bufferDefinition match {
          case x: ApplyBufferDefinition =>
            val (reducerDefsForThis, states) = x.argumentStatesForThisApply.partition(_.counts)
            val reducersForThis = reducerDefsForThis.map(argStateDef => findDownstreamArgumentStateBuffer(i, argStateDef.id))
            new MorselApplyBuffer(tracker,
                                  states.map(_.id),
                                  reducersForThis,
                                  reducers,
                                  workCancellers,
                                  argumentStateMaps,
                                  stateFactory.newBuffer(),
                                  x.argumentSlotOffset,
                                  stateFactory.newIdAllocator())

          case x: ArgumentStateBufferDefinition =>
            new MorselArgumentStateBuffer(tracker,
                                          reducers,
                                          workCancellers,
                                          argumentStateMaps,
                                          x.reducingPlanId)

          case _: BufferDefinition =>
            new MorselBuffer(tracker,
                             reducers,
                             workCancellers,
                             argumentStateMaps,
                             stateFactory.newBuffer())
        }
    }
    array
  }

  def sink(bufferId: BufferId): Sink[MorselExecutionContext] = buffers(bufferId.x)

  def hasData(bufferId: BufferId): Boolean = buffers(bufferId.x).asInstanceOf[Source[_]].hasData

  def morselBuffer(bufferId: BufferId): MorselBuffer =
    buffers(bufferId.x).asInstanceOf[MorselBuffer]

  def argumentStateBuffer[ACC <: MorselAccumulator](bufferId: BufferId): MorselArgumentStateBuffer[ACC] =
    buffers(bufferId.x).asInstanceOf[MorselArgumentStateBuffer[ACC]]
}
