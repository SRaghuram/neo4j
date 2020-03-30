/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.AbstractArgumentStateMap.ImmutableStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentStateController

/**
 * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
 *
 * This is an unordered argument state map.
 */
class ConcurrentArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                         val argumentSlotOffset: Int,
                                                         factory: ArgumentStateFactory[STATE])
  extends AbstractArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]] {

  override protected val controllers = new java.util.concurrent.ConcurrentHashMap[Long, AbstractArgumentStateMap.StateController[STATE]]()

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      new ImmutableStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
    } else {
      new ConcurrentStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers), initialCount)
    }
  }

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    val controller = controllers.get(argumentRowId)
    // The controller can be null if it was already removed due to a cancellation of that argument row id
    if (controller != null) {
      // This increment and decrement serves as a soft lock to make sure that if we apply `onState`, the modified state will be taken.
      if (controller.increment() >= 0) {
        // If we incremented before a different Thread tried to take, it won't be able to take until the update is performed and we decremented,
        // thus it will take the updated state.
        // If, on the other hand, the other Thread took before our increment, we will not enter this if block, and the update, including any side effects like incrementing counts,
        // won't be performed.
        DebugSupport.ASM.log("ASM %s update %03d", argumentStateMapId, argumentRowId)
        onState(controller.state)
      }
      controller.decrement()
    }
  }
}

object ConcurrentArgumentStateMap {
  /**
   * CAS the count to this value once taken.
   */
  private val TAKEN = -1000000

  /**
   * Controller which knows when an [[ArgumentState]] is complete,
   * and protects it from concurrent access.
   */
  private[state] class ConcurrentStateController[STATE <: ArgumentState](override val state: STATE, initialCount: Int)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private val count = new AtomicLong(initialCount)

    override def increment(): Long = count.incrementAndGet()

    override def decrement(): Long = count.decrementAndGet()

    override def tryTake(): Boolean = count.compareAndSet(0, TAKEN)

    override def take(): Boolean = count.getAndSet(TAKEN) >= 0

    override def isZero: Boolean = count.get() == 0

    override def toString: String = {
      s"[count: ${count.get()}, state: $state]"
    }
  }
}
