/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.{FilteringMorselExecutionContext, Morsel, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Buffers.SinkByOrigin

/**
 * A buffer which groups input morsels by argument row id and folds each
  * such group/view into an attachment of a new morsel.
 */
class MorselAttachBuffer(id: BufferId,
                         delegateApplyBuffer: MorselApplyBuffer,
                         outputSlots: SlotConfiguration,
                         argumentSlotOffset: Int,
                         argumentNumLongs: Int,
                         argumentNumRefs: Int
                       ) extends SinkByOrigin
                            with Sink[MorselExecutionContext] {

  override def sinkFor[T <: AnyRef](fromPipeline: PipelineId): Sink[T] = this.asInstanceOf[Sink[T]]

  def put(morsel: MorselExecutionContext): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }

    if (morsel.hasData) {
      ArgumentStateMap.foreach(argumentSlotOffset, morsel, (_, view) => {
        val outputMorsel = new FilteringMorselExecutionContext(Morsel.create(outputSlots, 1), outputSlots, 1, 0, 0, 1, morsel.producingWorkUnitEvent)
        outputMorsel.copyFrom(view, argumentNumLongs, argumentNumRefs)
        outputMorsel.attach(view)

        delegateApplyBuffer.put(outputMorsel)
      })
    }
  }

  override def canPut: Boolean = delegateApplyBuffer.canPut

  override def toString: String = s"MorselAttachBuffer($id, delegateApplyBuffer=$delegateApplyBuffer)"
}
