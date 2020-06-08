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
      val state = nextIfCompletedOrNull((state, isCompleted) => if (isCompleted) state else null.asInstanceOf[STATE])
      if (state == null) {
        nextIsComplete = false
      } else {
        builder += state
      }
    }
    if (builder.isEmpty)
      null.asInstanceOf[IndexedSeq[STATE]]
    else
      builder
  }

  override def takeOneIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    nextIfCompletedOrNull((state, isCompleted) => ArgumentStateWithCompleted(state, isCompleted))
  }

  private def nextIfCompletedOrNull[T](stateMapper: (STATE, Boolean) => T): T = {
    val controller = controllers.get(lastCompletedArgumentId + 1)
    if (controller != null) {
      if (controller.tryTake()) {
        lastCompletedArgumentId += 1
        controllers.remove(controller.state.argumentRowId)
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, controller.state.argumentRowId)
        stateMapper(controller.state, true)
      } else {
        stateMapper(controller.state, false)
      }
    } else {
      null.asInstanceOf[T]
    }
  }

  override def someArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    val controller = controllers.get(lastCompletedArgumentId + 1)
    controller != null && (controller.isZero || statePredicate(controller.state))
  }
}
