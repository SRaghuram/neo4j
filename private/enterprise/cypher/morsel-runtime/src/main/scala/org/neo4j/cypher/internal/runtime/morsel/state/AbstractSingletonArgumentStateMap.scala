/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateWithCompleted}

/**
  * A singleton argument state map, where singleton means it will only ever hold one STATE, of the argument 0. This is the
  * case for all argument state maps that are not on the RHS of any apply.
  */
abstract class AbstractSingletonArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]]
  extends ArgumentStateMapWithArgumentIdCounter[STATE] with ArgumentStateMapWithoutArgumentIdCounter[STATE] {

  override def argumentSlotOffset: Int = 0

  // ABSTRACT STUFF

  /**
    * The only controller.
    */
  protected var controller: CONTROLLER

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
    assert(argumentRowId == 0L)
    onState(controller.state)
  }

  override def clearAll(f: STATE => Unit): Unit = {
    if (controller != null && controller.take()) {
      // We do not remove the controller from controllers here in case there
      // is some outstanding work unit that wants to update count of accumulator
      f(controller.state)
    }
  }

  override def filter[U](morsel: MorselExecutionContext,
                         onArgument: (STATE, Long) => U,
                         onRow: (U, MorselExecutionContext) => Boolean): Unit = {
    val filterState = onArgument(controller.state, morsel.getValidRows)
    ArgumentStateMap.filter1(morsel,
                            row => onRow(filterState, row))
  }

  override def filterCancelledArguments(morsel: MorselExecutionContext,
                                        isCancelled: STATE => Boolean): IndexedSeq[Long] = {
    if (isCancelled(controller.state)) {
      morsel.moveToRow(morsel.getFirstRow)
      morsel.finishedWriting()
      IndexedSeq(0L)
    } else {
      IndexedSeq.empty
    }
  }

  override def takeOneCompleted(): STATE = {
    if (controller != null && controller.tryTake()) {
      val completedState = controller.state
      controller = null.asInstanceOf[CONTROLLER]
      DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, completedState.argumentRowId)
      return completedState
    }
    null.asInstanceOf[STATE]
  }

  override def takeNextIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    if (controller != null && controller.tryTake()) {
      lastCompletedArgumentId = 0L
      val completedState = controller.state
      controller = null.asInstanceOf[CONTROLLER]
      ArgumentStateWithCompleted(completedState, isCompleted = true)
    } else if (controller != null) {
      ArgumentStateWithCompleted(controller.state, isCompleted = false)
    } else {
      null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
    }
  }

  override def nextArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    controller != null && (controller.isZero || statePredicate(controller.state))
  }

  override def peekNext(): STATE = peek(lastCompletedArgumentId + 1)

  override def peekCompleted(): Iterator[STATE] = {
    if (controller != null && controller.isZero) Iterator.single(controller.state)
    else Iterator.empty
  }

  override def peek(argumentId: Long): STATE = {
    if (argumentId == 0) {
      controller.state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    controller != null && controller.isZero
  }

  override def hasCompleted(argument: Long): Boolean = {
    assert(argument == 0)
    controller != null && controller.isZero
  }

  override def remove(argument: Long): Boolean = {
    DebugSupport.ASM.log("ASM %s rem %03d", argumentStateMapId, argument)
    if (argument == 0) {
      controller == null
      true
    } else {
      false
    }
  }

  override def initiate(argument: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): Unit = {
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    assert(argument == 0)
    controller = newStateController(argument, argumentMorsel, argumentRowIdsForReducers)
  }

  override def increment(argument: Long): Unit = {
    assert(argument == 0)
    val newCount = controller.increment()
    DebugSupport.ASM.log("ASM %s incr %03d to %d", argumentStateMapId, argument, newCount)
  }

  override def decrement(argument: Long): Boolean = {
    assert(argument == 0)
    val newCount = controller.decrement()
    DebugSupport.ASM.log("ASM %s decr %03d to %d", argumentStateMapId, argument, newCount)
    newCount == 0
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb ++= "ArgumentStateMap(\n"
    sb ++= s"0 -> $controller\n"
    sb += ')'
    sb.result()
  }
}


