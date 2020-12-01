/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.AbstractArgumentStateMap.Controllers
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardCompletedStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardStateController
import org.neo4j.internal.helpers.Numbers
import org.neo4j.kernel.impl.util.collection.HeapTrackingLongEnumerationList
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

/**
 * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
 *
 * This is an ordered argument state map. Order is kept by using a `LinkedHashMap`.
 * No methods need to be overridden because scheduling guarantees that argument states
 * will be completed in argument row id order.
 *
 * There is no unordered standard argument state map because the performance overhead of a `LinkedHashMap` is negligible.
 */
class StandardArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                       val argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[STATE],
                                                       memoryTracker: MemoryTracker,
                                                       morselSize: Int)
  extends AbstractArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]](memoryTracker) with Controllers[AbstractArgumentStateMap.StateController[STATE]] {

  private val controllerList: HeapTrackingLongEnumerationList[AbstractArgumentStateMap.StateController[STATE]] =
    HeapTrackingLongEnumerationList.create(memoryTracker, Numbers.ceilingPowerOfTwo(morselSize))

  override def forEachController(fun: (Long, AbstractArgumentStateMap.StateController[STATE]) => Unit): Unit =
    controllerList.foreach((l, v) => fun(l, v))

  override def controllersIterator(): util.Iterator[AbstractArgumentStateMap.StateController[STATE]] =
    controllerList.valuesIterator()

  override def removeController(key: Long): AbstractArgumentStateMap.StateController[STATE] =
    controllerList.remove(key)

  override def putController(key: Long,
                             value: AbstractArgumentStateMap.StateController[STATE]): AbstractArgumentStateMap.StateController[STATE] = {
    require(controllerList.lastKey + 1 == key)
    val oldValue = controllerList.get(key)
    controllerList.add(value)
    oldValue
  }

  override def getController(key: Long): AbstractArgumentStateMap.StateController[STATE] =
    controllerList.get(key)

  override def getFirstController: AbstractArgumentStateMap.StateController[STATE] = controllerList.getFirst

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int,
                                            memoryTracker: MemoryTracker): AbstractArgumentStateMap.StateController[STATE] = {
    val state = factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers, memoryTracker)
    val controller =
      if (factory.completeOnConstruction) {
        new StandardCompletedStateController(state)
      } else {
        new StandardStateController(state, initialCount)
      }
    memoryTracker.allocateHeap(controller.shallowSize + state.shallowSize)
    controller
  }
}

object StandardArgumentStateMap {
  final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[StandardArgumentStateMap[_]])

  /**
   * Controller which knows when an [[ArgumentState]] is complete.
   */
  private[state] class StandardStateController[STATE <: ArgumentState](private var state: STATE, initialCount: Int)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private var _count: Long = initialCount

    override def increment(): Long = {
      _count += 1
      _count
    }

    override def decrement(): Long = {
      _count -= 1
      _count
    }

    override def takeCompleted(): STATE = {
      if (_count == 0) {
        take()
      } else {
        null.asInstanceOf[STATE]
      }
    }

    override def take(): STATE = {
      if (state != null) {
        val returnState = state
        state = null.asInstanceOf[STATE]
        returnState
      } else {
        null.asInstanceOf[STATE]
      }
    }

    override def hasCompleted: Boolean = _count == 0 && state != null

    override def peek: STATE = state

    override def peekCompleted: STATE = {
      if (_count == 0) {
        state
      } else {
        null.asInstanceOf[STATE]
      }
    }

    override def toString: String = {
      s"[count: ${_count}, state: $state]"
    }

    override final def shallowSize: Long = StandardStateController.SHALLOW_SIZE
  }

  object StandardStateController {
    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[StandardStateController[ArgumentState]])
  }

  /**
   * A state controller that is immediately completed and does not allow any reference increments/decrements.
   *
   * This controller serves the use case when an argument state is constructed and immediately ready for consumption.
   * This is the case for, e.g. cartesian product, distinct, and limit. This `increment` and `decrement` will throw exceptions.
   */
  private[state] class StandardCompletedStateController[STATE <: ArgumentState](private var state: STATE)
    extends AbstractArgumentStateMap.StateController[STATE] {

    override def increment(): Long = throw new IllegalStateException(s"Cannot increment ${this.getClass.getSimpleName}")

    override def decrement(): Long = throw new IllegalStateException(s"Cannot decrement ${this.getClass.getSimpleName}")

    override def takeCompleted(): STATE = {
      val completedState = state
      state = null.asInstanceOf[STATE]
      completedState
    }

    override def take(): STATE = {
      val completedState = state
      state = null.asInstanceOf[STATE]
      completedState
    }

    override def hasCompleted: Boolean = state != null

    override def peek: STATE = state

    override def peekCompleted: STATE = state

    override def toString: String = {
      s"[completed, state: $state]"
    }

    override def shallowSize: Long = StandardCompletedStateController.SHALLOW_SIZE
  }

  object StandardCompletedStateController {
    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[StandardStateController[ArgumentState]])
  }
}
