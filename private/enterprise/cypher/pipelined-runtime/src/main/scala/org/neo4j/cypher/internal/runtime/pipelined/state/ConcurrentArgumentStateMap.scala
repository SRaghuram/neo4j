/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentCompletedStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentStateController
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
 *
 * This is an unordered argument state map.
 */
class ConcurrentArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                         val argumentSlotOffset: Int,
                                                         factory: ArgumentStateFactory[STATE])
  extends AbstractArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]](EmptyMemoryTracker.INSTANCE) {

  private[this] val controllers = new ConcurrentHashMap[Long, AbstractArgumentStateMap.StateController[STATE]]

  override def forEachController(fun: (Long, AbstractArgumentStateMap.StateController[STATE]) => Unit): Unit = {
    controllers.forEach((arg, state) => fun(arg, state))
  }

  override def putController(key: Long, controller: AbstractArgumentStateMap.StateController[STATE]): AbstractArgumentStateMap.StateController[STATE] = {
    controllers.put(key, controller)
  }

  override def getController(key: Long): AbstractArgumentStateMap.StateController[STATE] =
    controllers.get(key)

  override def removeController(key: Long): AbstractArgumentStateMap.StateController[STATE] =
    controllers.remove(key)

  override def controllersIterator(): util.Iterator[AbstractArgumentStateMap.StateController[STATE]] = {
    controllers.values().iterator()
  }

  override def getFirstController: AbstractArgumentStateMap.StateController[STATE] = {
    // TODO: I think this is unusable. The method only makes sense on an OrderedConcurrentArgumentStateMap
    val iter = controllers.values.iterator()

    if(iter.hasNext) {
      iter.next()
    } else {
      null
    }
  }

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int,
                                            memoryTracker: MemoryTracker): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      ConcurrentCompletedStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
    } else {
      new ConcurrentStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers), initialCount)
    }
  }

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    val controller = getController(argumentRowId)
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
    checkOnlyWhenAssertionsAreEnabled(state != null)

    private val count = new AtomicLong(initialCount)
    private val peekerCount = new AtomicLong(0)

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

    override def trackedPeek: STATE = {
      peekerCount.incrementAndGet() // Increment here in order to protect against race with takeCompletedExclusive
      if (count.get() >= 0) {
        state
      } else {
        peekerCount.decrementAndGet()
        null.asInstanceOf[STATE]
      }
    }

    override def unTrackPeek: Unit = {
      peekerCount.decrementAndGet()
    }

    override def takeCompletedExclusive: STATE = {
      if (count.compareAndSet(0, TAKEN)) {
        if (peekerCount.get() == 0) {
          val returnState = state
          state = null.asInstanceOf[STATE]
          returnState
        } else {
          count.set(0)
          null.asInstanceOf[STATE]
        }
      } else {
        null.asInstanceOf[STATE]
      }
    }

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

    override def shallowSize: Long = ConcurrentStateController.SHALLOW_SIZE
  }

  object ConcurrentStateController {
    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentStateController[ArgumentState]])
  }

  /**
   * A state controller that is immediately completed and does not allow any reference increments/decrements.
   *
   * This controller serves the use case when an argument state is constructed and immediately ready for consumption.
   * This is the case for, e.g. cartesian product, distinct, and limit. This `increment` and `decrement` will throw exceptions.
   */
  private[state] class ConcurrentCompletedStateController[STATE <: ArgumentState] private (private val atomicState: AtomicReference[STATE])
    extends AbstractArgumentStateMap.StateController[STATE] {
    checkOnlyWhenAssertionsAreEnabled(atomicState.get() != null)

    private val peekerCount = new AtomicLong(0)

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

    override def shallowSize: Long = ConcurrentCompletedStateController.SHALLOW_SIZE

    override def trackedPeek: STATE = {
      peekerCount.incrementAndGet() // Increment here in order to protect against race with takeCompletedExclusive
      val state = atomicState.get()
      if (state != null) {
        state
      } else {
        peekerCount.decrementAndGet()
        null.asInstanceOf[STATE]
      }
    }

    override def unTrackPeek: Unit = {
      peekerCount.decrementAndGet()
    }

    override def takeCompletedExclusive: STATE = {
      val state = atomicState.getAndSet(null.asInstanceOf[STATE])
      if (state != null) {
        if (peekerCount.get() == 0) {
          state
        } else {
          atomicState.set(state)
          null.asInstanceOf[STATE]
        }
      } else {
        null.asInstanceOf[STATE]
      }
    }
  }

  object ConcurrentCompletedStateController {
    def apply[STATE <: ArgumentState](state: STATE): ConcurrentCompletedStateController[STATE] = new ConcurrentCompletedStateController(new AtomicReference(state))

    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentStateController[ArgumentState]])
  }
}
