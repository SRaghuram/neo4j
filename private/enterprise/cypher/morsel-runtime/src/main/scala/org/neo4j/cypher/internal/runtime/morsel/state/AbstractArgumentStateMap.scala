/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateWithCompleted}
import scala.collection.JavaConverters._

/**
  * All functionality of either standard or concurrent ASM that can be written without knowing the concrete Map type.
  */
abstract class AbstractArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]]
  extends ArgumentStateMapWithArgumentIdCounter[STATE] with ArgumentStateMapWithoutArgumentIdCounter[STATE] {

  /**
    * A Map of the controllers.
    */
  protected val controllers: java.util.Map[Long, CONTROLLER]

  // Not assigned here since the Concurrent implementation needs to declare this volatile
  /**
    * A private counter for the methods [[takeNextIfCompletedOrElsePeek()]], [[nextArgumentStateIsCompletedOr()]], and [[peekNext()]]
    */
  protected var lastCompletedArgumentId: Long

  /**
    * Create a new state controller
    */
  protected def newStateController(argument: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): CONTROLLER

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    onState(controllers.get(argumentRowId).state)
  }

  override def clearAll(f: STATE => Unit): Unit = {
    controllers.forEach((_, controller) => {
      if (controller.take()) {
        // We do not remove the controller from controllers here in case there
        // is some outstanding work unit that wants to update count of accumulator
        f(controller.state)
      }
    })
  }

  override def filter[U](readingRow: MorselExecutionContext,
                         onArgument: (STATE, Long) => U,
                         onRow: (U, MorselExecutionContext) => Boolean): Unit = {
    ArgumentStateMap.filter(
      argumentSlotOffset,
      readingRow,
      (argumentRowId, nRows) =>
        onArgument(controllers.get(argumentRowId).state, nRows),
      onRow
    )
  }

  override def filterCancelledArguments(morsel: MorselExecutionContext,
                                        isCancelled: STATE => Boolean): IndexedSeq[Long] = {
    ArgumentStateMap.filterCancelledArguments(argumentSlotOffset,
      morsel,
      argumentRowId => isCancelled(controllers.get(argumentRowId).state))
  }

  override def takeOneCompleted(): STATE = {
    val iterator = controllers.values().iterator()

    while(iterator.hasNext) {
      val controller = iterator.next()
      if (controller.tryTake()) {
        controllers.remove(controller.state.argumentRowId)
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, controller.state.argumentRowId)
        return controller.state
      }
    }
    null.asInstanceOf[STATE]
  }

  override def takeNextIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    val controller = controllers.get(lastCompletedArgumentId + 1)
    if (controller != null) {
      if (controller.tryTake()) {
        lastCompletedArgumentId += 1
        controllers.remove(controller.state.argumentRowId)
        ArgumentStateWithCompleted(controller.state, isCompleted = true)
      } else {
        ArgumentStateWithCompleted(controller.state, isCompleted = false)
      }
    } else {
      null.asInstanceOf[ArgumentStateWithCompleted[STATE]]
    }
  }

  override def nextArgumentStateIsCompletedOr(statePredicate: STATE => Boolean): Boolean = {
    val controller = controllers.get(lastCompletedArgumentId + 1)
    controller != null && (controller.isZero || statePredicate(controller.state))
  }

  override def peekNext(): STATE = peek(lastCompletedArgumentId + 1)

  override def peekCompleted(): Iterator[STATE] = {
    controllers.values().stream().filter(_.isZero).map[STATE](_.state).iterator().asScala
  }

  override def peek(argumentId: Long): STATE = {
    val controller = controllers.get(argumentId)
    if (controller != null) {
      controller.state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    val iterator = controllers.values().iterator()

    while(iterator.hasNext) {
      val controller = iterator.next()
      if (controller.isZero) {
        return true
      }
    }
    false
  }

  override def hasCompleted(argument: Long): Boolean = {
    val controller = controllers.get(argument)
    controller != null && controller.isZero
  }

  override def remove(argument: Long): Boolean = {
    DebugSupport.ASM.log("ASM %s rem %03d", argumentStateMapId, argument)
    controllers.remove(argument) != null
  }

  override def initiate(argument: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): Unit = {
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    val newController = newStateController(argument, argumentMorsel, argumentRowIdsForReducers)
    controllers.put(argument, newController)
  }

  override def increment(argument: Long): Unit = {
    val controller = controllers.get(argument)
    val newCount = controller.increment()
    DebugSupport.ASM.log("ASM %s incr %03d to %d", argumentStateMapId, argument, newCount)
  }

  override def decrement(argument: Long): STATE = {
    val controller = controllers.get(argument)
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
      * The state the controller is holding.
      */
    def state: STATE

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
      * @return if the count is zero.
      */
    def isZero: Boolean

    /**
      * Atomically tries to take the controller. The implementation must guarantee that taking can only happen once.
      * @return if this call succeeded in taking the controller
      */
    def take(): Boolean

    /**
      * Atomically tries to take the controller if the count is zero. The implementation must guarantee that taking can only happen once.
      * @return if this call succeeded in taking the controller
      */
    def tryTake(): Boolean
  }
}
