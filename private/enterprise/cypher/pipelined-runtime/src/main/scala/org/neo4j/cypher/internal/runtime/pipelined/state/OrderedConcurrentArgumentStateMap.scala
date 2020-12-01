/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted

import scala.collection.mutable.ArrayBuffer

/**
 * This is an ordered argument state map. Order is kept by `lastCompletedArgumentId` and overriding all relevant methods.
 */
class OrderedConcurrentArgumentStateMap[STATE <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                                argumentSlotOffset: Int,
                                                                factory: ArgumentStateFactory[STATE])
  extends ConcurrentArgumentStateMap[STATE](argumentStateMapId, argumentSlotOffset, factory) {

  @volatile
  protected var lastCompletedArgumentId: Long = -1

  override def takeCompleted(n: Int): IndexedSeq[STATE] = {
    var nextIsComplete = true
    val builder = new ArrayBuffer[STATE]

    while(nextIsComplete && builder.size < n) {
      val state = takeOneIfCompletedOrElsePeek()
      if (state != null && state.isCompleted) {
        builder += state.argumentState
      } else {
        nextIsComplete = false
      }
    }
    if (builder.isEmpty)
      null.asInstanceOf[IndexedSeq[STATE]]
    else
      builder
  }

  override def takeOneIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    val controller = getController(lastCompletedArgumentId + 1)
    if (controller != null) {
      val state = controller.takeCompleted()
      if (state != null) {
        lastCompletedArgumentId += 1
        removeController(state.argumentRowId)
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, state.argumentRowId)
        ArgumentStateWithCompleted(state, isCompleted = true)
      } else {
        val peekedState = controller.peek
        if (peekedState != null) {
          ArgumentStateWithCompleted(peekedState, isCompleted = false)
        } else {
          null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
        }
      }
    } else {
      null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
    }
  }

  override def someArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    val controller = getController(lastCompletedArgumentId + 1)
    controller != null && (controller.hasCompleted || statePredicate(controller.peek))
  }
}
