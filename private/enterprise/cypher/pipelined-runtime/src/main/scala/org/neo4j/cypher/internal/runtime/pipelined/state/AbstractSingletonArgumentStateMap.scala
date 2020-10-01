/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

/**
 * A singleton argument state map, where singleton means it will only ever hold one STATE, of the argument 0. This is the
 * case for all argument state maps that are not on the RHS of any apply.
 *
 * This is per definition an ordered argument state map.
 */
abstract class AbstractSingletonArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]]
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
  protected def newStateController(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): CONTROLLER

  // ARGUMENT STATE MAP FUNCTIONALITY

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    TopLevelArgument.assertTopLevelArgument(argumentRowId)
    onState(controller.state)
  }

  override def clearAll(f: STATE => Unit): Unit = {
    if (hasController && controller.take()) {
      // We do not remove the controller from controllers here in case there
      // is some outstanding work unit that wants to update count of accumulator
      f(controller.state)
    }
  }


  override def skip(morsel: Morsel,
                    reserve: (STATE, Int) => Int): Unit = {
    val end = reserve(controller.state, morsel.numberOfRows)

    ArgumentStateMap.skip(morsel, end)
  }

  override def filterWithSideEffect[U](morsel: Morsel,
                                       createState: (STATE, Int) => U,
                                       predicate: (U, ReadWriteRow) => Boolean): Unit = {
    val filterState = createState(controller.state, morsel.numberOfRows)
    ArgumentStateMap.filter(morsel,
      row => predicate(filterState, row))
  }

  override def takeCompleted(n: Int): IndexedSeq[STATE] = {
    if (hasController && controller.tryTake()) {
      val completedState = controller.state
      hasController = false
      DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
      return Collections.singletonIndexedSeq(completedState)
    }
    null
  }

  override def takeIfCompletedOrElsePeek(argumentId: Long): ArgumentStateWithCompleted[STATE] = {
    takeOneIfCompletedOrElsePeek()
  }

  override def takeOneIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    if (hasController) {
      if (controller.tryTake()) {
        val completedState = controller.state
        hasController = false
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
        return ArgumentStateWithCompleted(completedState, isCompleted = true)
      } else {
        return ArgumentStateWithCompleted(controller.state, isCompleted = false)
      }
    }
    null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
  }

  override def someArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    hasController && (controller.isZero || statePredicate(controller.state))
  }

  override def exists(statePredicate: STATE => Boolean): Boolean = {
    hasController && statePredicate(controller.state)
  }

  override def peekCompleted(): Iterator[STATE] = {
    if (hasController && controller.isZero) Iterator.single(controller.state)
    else Iterator.empty
  }

  override def peek(argumentId: Long): STATE = {
    TopLevelArgument.assertTopLevelArgument(argumentId)
    if (hasController) {
      controller.state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    hasController && controller.isZero
  }

  override def hasCompleted(argument: Long): Boolean = {
    TopLevelArgument.assertTopLevelArgument(argument)
    hasController && controller.isZero
  }

  override def remove(argument: Long): STATE = {
    TopLevelArgument.assertTopLevelArgument(argument)
    DebugSupport.ASM.log("ASM %s rem %03d", argumentStateMapId, argument)
    hasController = false
    controller.state
  }

  override def initiate(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): Unit = {
    TopLevelArgument.assertTopLevelArgument(argument)
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    controller = newStateController(argument, argumentMorsel, argumentRowIdsForReducers, initialCount)
  }

  override def increment(argument: Long): Unit = {
    TopLevelArgument.assertTopLevelArgument(argument)
    val newCount = controller.increment()
    DebugSupport.ASM.log("ASM %s incr %03d to %d", argumentStateMapId, argument, newCount)
  }

  override def decrement(argument: Long): STATE = {
    TopLevelArgument.assertTopLevelArgument(argument)
    val newCount = controller.decrement()
    DebugSupport.ASM.log("ASM %s decr %03d to %d", argumentStateMapId, argument, newCount)
    if (newCount == 0) {
      controller.state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb ++= "ArgumentStateMap(\n"
    sb ++= s"0 -> ${if (hasController) controller else null}\n"
    sb += ')'
    sb.result()
  }
}


