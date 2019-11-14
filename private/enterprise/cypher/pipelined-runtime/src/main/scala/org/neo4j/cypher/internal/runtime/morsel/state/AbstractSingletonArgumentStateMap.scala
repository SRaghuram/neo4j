/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateWithCompleted}

/**
  * A singleton argument state map, where singleton means it will only ever hold one STATE, of the argument 0. This is the
  * case for all argument state maps that are not on the RHS of any apply.
  */
abstract class AbstractSingletonArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]]
  extends ArgumentStateMapWithArgumentIdCounter[STATE] with ArgumentStateMapWithoutArgumentIdCounter[STATE] {

  override def argumentSlotOffset: Int = TopLevelArgument.SLOT_OFFSET

  // ABSTRACT STUFF

  /**
    * The only controller.
    */
  protected var controller: CONTROLLER

  protected var hasController: Boolean

  // Not assigned here since the Concurrent implementation needs to declare this volatile
  /**
    * A private counter for the methods [[takeNextIfCompletedOrElsePeek()]], [[nextArgumentStateIsCompletedOr()]], and [[peekNext()]]. Will only be -1 or 0.
    */
  protected var lastCompletedArgumentId: Long

  /**
    * Create a new state controller
    */
  protected def newStateController(argument: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): CONTROLLER

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

  override def filter[U](morsel: MorselExecutionContext,
                         onArgument: (STATE, Long) => U,
                         onRow: (U, MorselExecutionContext) => Boolean): Unit = {
    val filterState = onArgument(controller.state, morsel.getValidRows)
    ArgumentStateMap.filter(morsel,
                            row => onRow(filterState, row))
  }

  override def takeOneCompleted(): STATE = {
    if (hasController && controller.tryTake()) {
      val completedState = controller.state
      hasController = false
      DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
      return completedState
    }
    null.asInstanceOf[STATE]
  }

  override def takeNextIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    if (hasController && controller.tryTake()) {
      lastCompletedArgumentId = TopLevelArgument.VALUE
      val completedState = controller.state
      hasController = false
      ArgumentStateWithCompleted(completedState, isCompleted = true)
    } else if (hasController) {
      ArgumentStateWithCompleted(controller.state, isCompleted = false)
    } else {
      null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
    }
  }

  override def nextArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    hasController && (controller.isZero || statePredicate(controller.state))
  }

  override def peekNext(): STATE = peek(lastCompletedArgumentId + 1)

  override def peekCompleted(): Iterator[STATE] = {
    if (hasController && controller.isZero) Iterator.single(controller.state)
    else Iterator.empty
  }

  override def peek(argumentId: Long): STATE = {
    if (argumentId == TopLevelArgument.VALUE) {
      controller.state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    hasController && controller.isZero
  }

  override def hasCompleted(argument: Long): Boolean = {
    argument == TopLevelArgument.VALUE && controller != null && controller.isZero
  }

  override def remove(argument: Long): Boolean = {
    DebugSupport.ASM.log("ASM %s rem %03d", argumentStateMapId, argument)
    if (argument == TopLevelArgument.VALUE) {
      hasController = false
      true
    } else {
      false
    }
  }

  override def initiate(argument: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): Unit = {
    TopLevelArgument.assertTopLevelArgument(argument)
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    controller = newStateController(argument, argumentMorsel, argumentRowIdsForReducers)
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


