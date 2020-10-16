/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentCompletedStateController
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
      ConcurrentCompletedStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
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
        onState(controller.peek)
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
  private[state] class ConcurrentStateController[STATE <: ArgumentState](private var state: STATE, initialCount: Int)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private val count = new AtomicLong(initialCount)

    override def increment(): Long = count.incrementAndGet()

    override def decrement(): Long = count.decrementAndGet()

    override def takeCompleted(): STATE = {
      if (count.compareAndSet(0, TAKEN)) {
        val returnState = state
        state = null.asInstanceOf[STATE]
        returnState
      } else {
        null.asInstanceOf[STATE]
      }
    }

    override def take(): STATE = {
      if (count.getAndSet(TAKEN) >= 0) {
        val returnState = state
        state = null.asInstanceOf[STATE]
        returnState
      } else {
        null.asInstanceOf[STATE]
      }
    }

    override def hasCompleted: Boolean = count.get() == 0

    override def peek: STATE = state

    override def peekCompleted: STATE = {
      if (count.get() == 0) {
        // TODO: There is a potential race condition here where it's possible to return the (non null) state
        //       even if it's taken (taker hasn't completed setting it to null). Is it ok to give no guarantees here?
        state
      } else {
        null.asInstanceOf[STATE]
      }
    }

    override def toString: String = {
      s"[count: ${count.get()}, state: $state]"
    }
  }

  /**
   * A state controller that is immediately completed and does not allow any reference increments/decrements.
   *
   * This controller serves the use case when an argument state is constructed and immediately ready for consumption.
   * This is the case for, e.g. cartesian product, distinct, and limit. This `increment` and `decrement` will throw exceptions.
   */
  private[state] class ConcurrentCompletedStateController[STATE <: ArgumentState] private (private val atomicState: AtomicReference[STATE])
    extends AbstractArgumentStateMap.StateController[STATE] {

    override def increment(): Long = throw new IllegalStateException(s"Cannot increment ${this.getClass.getSimpleName}")

    override def decrement(): Long = throw new IllegalStateException(s"Cannot decrement ${this.getClass.getSimpleName}")

    override def takeCompleted(): STATE = {
      atomicState.getAndSet(null.asInstanceOf[STATE])
    }

    override def take(): STATE = {
      atomicState.getAndSet(null.asInstanceOf[STATE])
    }

    override def hasCompleted: Boolean = atomicState.get() != null

    override def peek: STATE = atomicState.get()

    override def peekCompleted: STATE = atomicState.get()

    override def toString: String = {
      s"[completed, state: ${atomicState.get()}]"
    }
  }

  object ConcurrentCompletedStateController {
    def apply[STATE <: ArgumentState](state: STATE): ConcurrentCompletedStateController[STATE] = new ConcurrentCompletedStateController(new AtomicReference(state))
  }
}
