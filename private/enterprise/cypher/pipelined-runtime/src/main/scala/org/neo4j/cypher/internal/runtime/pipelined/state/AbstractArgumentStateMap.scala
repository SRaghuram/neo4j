/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.memory.MemoryTracker
import org.neo4j.util.Preconditions

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

/**
 * All functionality of either standard or concurrent ASM that can be written without knowing the concrete Map type.
 */
abstract class AbstractArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]](memoryTracker: MemoryTracker)
  extends ArgumentStateMap[STATE] {

  trait Controllers[V] {
    /**
     * Calls the given function on all entries in the map.
     * @param fun the function
     */
    def forEach(fun: (Long, V) => Unit)

    /**
     * Adds an entry to the map.
     * @param key the key
     * @param value the value
     * @return
     */
    def put(key: Long, value: V): V

    /**
     * @param key the entry key
     * @return the value to which the given key is mapped
     */
    def get(key: Long): V

    /**
     * Removes the entry with given key and asserts that it is the first entry.
     * @param key the key
     * @return the value of the removed entry
     */
    def remove(key: Long): V

    /**
     * @return the first value or null if no value exists
     */
    def getFirstValue: V

    /**
     * @return an iterator over all values
     */
    def valuesIterator(): java.util.Iterator[V]
  }

  /**
   * A Map of the controllers.
   */
    protected val controllers: Controllers[CONTROLLER]

  /**
   * Create a new state controller
   *
   * @param initialCount the initial count for the controller
   */
  protected def newStateController(argument: Long,
                                   argumentMorsel: MorselReadCursor,
                                   argumentRowIdsForReducers: Array[Long],
                                   initialCount: Int,
                                   memoryTracker: MemoryTracker): CONTROLLER

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    DebugSupport.ASM.log("ASM %s update %03d", argumentStateMapId, argumentRowId)
    onState(controllers.get(argumentRowId).peek)
  }

  override def clearAll(f: STATE => Unit): Unit = {
    controllers.forEach((_, controller) => {
      val state = controller.take()
      if (state != null) {
        // We do not remove the controller from controllers here in case there
        // is some outstanding work unit that wants to update count of accumulator
        f(state)
      }
    })
  }

  override def skip(morsel: Morsel,
                    reserve: (STATE, Int) => Int): Unit = {
    ArgumentStateMap.skip(
      argumentSlotOffset,
      morsel,
      (argumentRowId, nRows) => reserve(controllers.get(argumentRowId).peek, nRows))
  }

  override def filterWithSideEffect[FILTER_STATE](morsel: Morsel,
                                       createState: (STATE, Int) => FilterStateWithIsLast[FILTER_STATE],
                                       predicate: (FILTER_STATE, ReadWriteRow) => Boolean): Unit = {
    ArgumentStateMap.filter(
      argumentSlotOffset,
      morsel,
      (argumentRowId, nRows) => {
        val controller = controllers.get(argumentRowId)
        if (controller != null) {
          val controllerState = controller.peek
          if (controllerState != null) {
            val FilterStateWithIsLast(filterState, isLastState) = createState(controllerState, nRows)
            if (isLastState) {
              if (controller.hasCompleted) {
                controllers.remove(argumentRowId) // Saves memory
              }
              controller.take() // Saves memory
            }
            filterState
          } else {
            // Controller state is taken => discard all rows for this argument row id
            null.asInstanceOf[FILTER_STATE]
          }
        } else {
          // Controller is null => discard all rows for this argument row id
          null.asInstanceOf[FILTER_STATE]
        }
      },
      predicate
      )
  }

  override def takeCompleted(n: Int): IndexedSeq[STATE] = {
    val iterator = controllers.valuesIterator()
    val builder = new ArrayBuffer[STATE]

    while(iterator.hasNext && builder.size < n) {
      val controller = iterator.next()
      val completedState = controller.takeCompleted()
      if (completedState != null) {
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
        builder += completedState
      }
    }

    var i = 0
    while (i < builder.length) {
      val controller = controllers.remove(builder(i).argumentRowId)
      memoryTracker.releaseHeap(controller.shallowSize)

      i += 1
    }

    if (builder.isEmpty) {
      null.asInstanceOf[IndexedSeq[STATE]]
    } else {
      builder
    }
  }

  override def takeIfCompletedOrElsePeek(argumentId: Long): ArgumentStateWithCompleted[STATE] = {
    val controller = controllers.get(argumentId)
    if (controller == null) {
      null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
    } else {
      val completedState = controller.takeCompleted()
      if (completedState != null) {
        controllers.remove(completedState.argumentRowId)
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
        ArgumentStateWithCompleted(completedState, isCompleted = true)
      } else {
        val peekedState = controller.peek
        if (peekedState != null) {
          ArgumentStateWithCompleted(peekedState, isCompleted = false)
        } else {
          null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
        }
      }
    }
  }

  override def takeOneIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    val controller = controllers.getFirstValue

    if (controller != null) {
      val completedState = controller.takeCompleted()
      if (completedState != null) {
        controllers.remove(completedState.argumentRowId)
        memoryTracker.releaseHeap(controller.shallowSize)
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
        ArgumentStateWithCompleted(completedState, isCompleted = true)
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

  override def peekCompleted(): Iterator[STATE] = {
    controllers.valuesIterator().asScala.map[STATE](_.peekCompleted).filter(_ != null)
  }

  override def peek(argumentId: Long): STATE = {
    val controller = controllers.get(argumentId)
    if (controller != null) {
      controller.peek
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    val iterator = controllers.valuesIterator()

    while(iterator.hasNext) {
      val controller = iterator.next()
      if (controller.hasCompleted) {
        return true
      }
    }
    false
  }

  override def someArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    val iterator = controllers.valuesIterator()

    while(iterator.hasNext) {
      val controller = iterator.next()
      if (controller.hasCompleted || statePredicate(controller.peek)) {
        return true
      }
    }
    false
  }

  override def hasCompleted(argument: Long): Boolean = {
    val controller = controllers.get(argument)
    controller != null && controller.hasCompleted
  }

  override def exists(statePredicate: STATE => Boolean): Boolean = {
    val iterator = controllers.valuesIterator()

    while(iterator.hasNext) {
      val controller = iterator.next()
      val state = controller.peek
      if (state != null && statePredicate(state)) {
        return true
      }
    }
    false
  }

  override def remove(argument: Long): STATE = {
    DebugSupport.ASM.log("ASM %s rem %03d", argumentStateMapId, argument)
    val controller = controllers.remove(argument)
    if (controller != null) {
      val state = controller.take()
      memoryTracker.releaseHeap(controller.shallowSize)
      state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def initiate(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): Unit = {
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    val newController = newStateController(argument, argumentMorsel, argumentRowIdsForReducers, initialCount, memoryTracker)
    val previousValue = controllers.put(argument, newController)
    memoryTracker.allocateHeap(newController.shallowSize)
    Preconditions.checkState(previousValue == null, "ArgumentStateMap cannot re-initiate the same argument (argument: %d)", Long.box(argument))
  }

  override def increment(argument: Long): Unit = {
    val controller = controllers.get(argument)
    val newCount = controller.increment()
    DebugSupport.ASM.log("ASM %s incr %03d to %d", argumentStateMapId, argument, newCount)
  }

  override def decrement(argument: Long): STATE = {
    val controller = controllers.get(argument)
    val statePeek = controller.peek // We peek before decrement to be sure to not loose the state if someone else takes it after decrement
    val newCount = controller.decrement()
    DebugSupport.ASM.log("ASM %s decr %03d to %d", argumentStateMapId, argument, newCount)
    if (newCount == 0) {
      statePeek
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override final def toString: String = {
    val sb = new StringBuilder
    sb ++= "ArgumentStateMap(\n"
    controllers.forEach((argumentRowId, controller) => {
      sb ++= s"$argumentRowId -> $controller\n"
    })
    sb += ')'
    sb.result()
  }
}

object AbstractArgumentStateMap {
  trait StateController[STATE <: ArgumentState] {

    /**
     * Increment the count.
     * @return the new count
     */
    def increment(): Long

    /**
     * Decrement the count.
     * @return the new count
     */
    def decrement(): Long

    /**
     * Atomically tries to take the controller and returns the state if it has not already been taken.
     * The implementation must guarantee that taking can only happen once.
     * @return the state if successful, otherwise null
     */
    def take(): STATE

    /**
     * Atomically tries to take the controller and returns the state if the count is zero _and_ the state has not already been taken.
     * The implementation must guarantee that taking can only happen once.
     * @return the state if successful, otherwise null
     */
    def takeCompleted(): STATE

    /**
     * Peeks the controller and returns the state it is holding, or null if the state has been taken.
     * @return the state if successful, otherwise null
     */
    def peek: STATE

    /**
     * Peeks the controller and returns the state if the count is zero _and_ the state has not already been taken.
     * @return the state if successful, otherwise null
     */
    def peekCompleted: STATE

    /**
     * @return true if the controller has a count of zero _and_ the state has not already been taken, otherwise false.
     */
    def hasCompleted: Boolean

    /**
     * @return the shallow size of the state controller
     */
    def shallowSize: Long
  }
}
