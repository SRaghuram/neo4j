/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.memory.MemoryTracker

/**
 * A singleton argument state map, where singleton means it will only ever hold one STATE, of the argument 0. This is the
 * case for all argument state maps that are not on the RHS of any apply.
 *
 * This is per definition an ordered argument state map.
 */
abstract class AbstractSingletonArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]](memoryTracker: MemoryTracker)
  extends ArgumentStateMap[STATE] {

  override def argumentSlotOffset: Int = TopLevelArgument.SLOT_OFFSET

  // ABSTRACT STUFF

  /**
   * The only controller.
   */
  protected var controller: CONTROLLER

  protected var hasController: Boolean

  /**
   * Create a new state controller
   *
   * @param initialCount the initial count for the argument row id
   */
  protected def newStateController(argument: Long,
                                   argumentMorsel: MorselReadCursor,
                                   argumentRowIdsForReducers: Array[Long],
                                   initialCount: Int,
                                   memoryTracker: MemoryTracker): CONTROLLER

  // ARGUMENT STATE MAP FUNCTIONALITY

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    TopLevelArgument.assertTopLevelArgument(argumentRowId)
    onState(controller.peek)
  }

  override def clearAll(f: STATE => Unit): Unit = {
    if (hasController) {
      val completedState = controller.take()
      if (completedState != null) {
        // We do not remove the controller from controllers here in case there
        // is some outstanding work unit that wants to update count of accumulator
        f(completedState)
      }
    }
  }


  override def skip(morsel: Morsel,
                    reserve: (STATE, Int) => Int): Unit = {
    val end = reserve(controller.peek, morsel.numberOfRows)

    ArgumentStateMap.skip(morsel, end)
  }

  override def filterWithSideEffect[FILTER_STATE](morsel: Morsel,
                                                  createState: (STATE, Int) => FilterStateWithIsLast[FILTER_STATE],
                                                  predicate: (FILTER_STATE, ReadWriteRow) => Boolean): Unit = {
    val controllerState = controller.peek
    val filterState =
      if (controllerState != null) {
        val FilterStateWithIsLast(state, _) = createState(controllerState, morsel.numberOfRows)
        state
      }
      else
        null.asInstanceOf[FILTER_STATE]

    ArgumentStateMap.filter(morsel,
      row => predicate(filterState, row))
  }

  override def takeCompleted(n: Int): IndexedSeq[STATE] = {
    if (hasController) {
      val completedState = controller.takeCompleted()
      if (completedState != null) {
        hasController = false
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
        return Collections.singletonIndexedSeq(completedState)
      }
    }
    null
  }

  override def takeIfCompletedOrElsePeek(argumentId: Long): ArgumentStateWithCompleted[STATE] = {
    takeOneIfCompletedOrElsePeek()
  }

  override def takeOneIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    if (hasController) {
      val completedState = controller.takeCompleted()
      if (completedState != null) {
        hasController = false
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

  override def someArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    hasController && (controller.hasCompleted || statePredicate(controller.peek))
  }

  override def exists(statePredicate: STATE => Boolean): Boolean = {
    hasController && {
      val state = controller.peek
      state != null && statePredicate(state)
    }
  }

  override def peekCompleted(): Iterator[STATE] = {
    if (hasController) {
      val completedState = controller.peekCompleted
      if (completedState != null) {
        Iterator.single(completedState)
      }
      else Iterator.empty
    }
    else Iterator.empty
  }

  override def peek(argumentId: Long): STATE = {
    TopLevelArgument.assertTopLevelArgument(argumentId)
    if (hasController) {
      controller.peek
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    hasController && controller.hasCompleted
  }

  override def hasCompleted(argument: Long): Boolean = {
    TopLevelArgument.assertTopLevelArgument(argument)
    controller != null && controller.hasCompleted
  }

  override def remove(argument: Long): STATE = {
    TopLevelArgument.assertTopLevelArgument(argument)
    DebugSupport.ASM.log("ASM %s rem %03d", argumentStateMapId, argument)
    hasController = false
    controller.take()
  }

  override def initiate(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): Unit = {
    TopLevelArgument.assertTopLevelArgument(argument)
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    controller = newStateController(argument, argumentMorsel, argumentRowIdsForReducers, initialCount, memoryTracker)
  }

  override def increment(argument: Long): Unit = {
    TopLevelArgument.assertTopLevelArgument(argument)
    val newCount = controller.increment()
    DebugSupport.ASM.log("ASM %s incr %03d to %d", argumentStateMapId, argument, newCount)
  }

  override def decrement(argument: Long): STATE = {
    TopLevelArgument.assertTopLevelArgument(argument)
    val statePeek = controller.peek // We peek before decrement to be sure to not loose the state if someone else takes it after decrement
    val newCount = controller.decrement()
    DebugSupport.ASM.log("ASM %s decr %03d to %d", argumentStateMapId, argument, newCount)
    if (newCount == 0) {
      statePeek
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def toString: String =
    s"ArgumentStateMap[$argumentStateMapId](0 -> ${if (hasController) controller else null})"
}


