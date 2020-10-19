/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFactory
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap

/**
 * A buffer which groups input morsels by argument row id. For each view a new morsel is created and the view is "attached" to the new morsel.
 * This Buffer is used for CartesianProduct. A Morsel allows to attach another morsel as form of communication, for this particular case.
 *
 * The new morsel is given to an ApplyBuffer which has a [[org.neo4j.cypher.internal.runtime.pipelined.operators.CartesianProductOperator.LHSMorsel]]
 * as one delegate (this is the LHS of the MrBuff that sits before the CartesianProduct) and the RHS leaf/leaves as more delegates.
 *
 * The LHSMorsel will detach the attachment morsel. The RHS operates on the new morsel, which consists of a single row. The CartesianProduct operator
 * then associates the right LHSMorsel with data from the RHS.
 *
 * The execution graph looks like this:
 *
 * lhs -> [ATTACH]-[APPLY]----------- ..... > [MrBuff LHS]
 * _____________________\-[DELEGATE]--rhs- > [MrBuff RHS] -CP,top->
 **/
class MorselAttachBuffer(id: BufferId,
                         delegateApplyBuffer: MorselApplyBuffer,
                         outputSlots: SlotConfiguration,
                         argumentSlotOffset: Int,
                         argumentNumLongs: Int,
                         argumentNumRefs: Int
                        ) extends Sink[Morsel] {

  def put(morsel: Morsel, resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }

    if (morsel.hasData) {
      ArgumentStateMap.foreach(argumentSlotOffset, morsel, (_, view) => {
        val outputMorsel = MorselFactory.allocateFiltering(outputSlots, 1, morsel.producingWorkUnitEvent)
        val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
        outputCursor.copyFrom(view.readCursor(onFirstRow = true), argumentNumLongs, argumentNumRefs)

        outputMorsel.attach(view)

        delegateApplyBuffer.put(outputMorsel, resources)
      })
    }
  }

  override def canPut: Boolean = delegateApplyBuffer.canPut

  override def toString: String = s"MorselAttachBuffer($id, delegateApplyBuffer=$delegateApplyBuffer)"
}
