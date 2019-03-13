/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.{ApplyBufferDefinition, ArgumentStateBufferDefinition, BufferId, BufferDefinition}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMaps, MorselAccumulator}

/**
  * Container for all buffers of the execution state.
  */
class Buffers(bufferDefinitions: Seq[BufferDefinition],
              tracker: QueryCompletionTracker,
              argumentStateMaps: ArgumentStateMaps,
              stateFactory: StateFactory) {

  private val buffers: Array[Sink[MorselExecutionContext]] =
    for (bufferDefinition <- bufferDefinitions.toArray) yield {
      val reducers = bufferDefinition.reducers
      bufferDefinition match {
        case x: ApplyBufferDefinition =>
          new MorselApplyBuffer(tracker, x.reducersForThisApply, reducers, argumentStateMaps, stateFactory.newBuffer(), x.argumentSlotOffset, stateFactory.newIdAllocator())

        case x: ArgumentStateBufferDefinition =>
          new MorselArgumentStateBuffer(tracker, reducers, argumentStateMaps, x.reducingPlanId)

        case _: BufferDefinition =>
          new MorselBuffer(tracker, reducers, argumentStateMaps, stateFactory.newBuffer())
      }
    }

  def sink(bufferId: BufferId): Sink[MorselExecutionContext] = buffers(bufferId.x)

  def morselBuffer(bufferId: BufferId): MorselBuffer =
    buffers(bufferId.x).asInstanceOf[MorselBuffer]

  def argumentStateMapBuffer[ACC <: MorselAccumulator](bufferId: BufferId): MorselArgumentStateBuffer[ACC] =
    buffers(bufferId.x).asInstanceOf[MorselArgumentStateBuffer[ACC]]
}
