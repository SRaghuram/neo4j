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
import org.neo4j.util.Preconditions

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * All functionality of either standard or concurrent ASM that can be written without knowing the concrete Map type.
 */
abstract class AbstractArgumentStateMap[STATE <: ArgumentState, CONTROLLER <: AbstractArgumentStateMap.StateController[STATE]]
  extends OrderedArgumentStateMap[STATE] with UnorderedArgumentStateMap[STATE] {

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
   *
   * @param initialCount the initial count for the controller
   */
  protected def newStateController(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): CONTROLLER

  override def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    DebugSupport.ASM.log("ASM %s update %03d", argumentStateMapId, argumentRowId)
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

  override def skip(morsel: Morsel,
                    reserve: (STATE, Int) => Int): Unit = {
    ArgumentStateMap.skip(
      argumentSlotOffset,
      morsel,
      (argumentRowId, nRows) => reserve(controllers.get(argumentRowId).state, nRows))
  }

  override def filterWithSideEffect[U](morsel: Morsel,
                                       createState: (STATE, Int) => U,
                                       predicate: (U, ReadWriteRow) => Boolean): Unit = {
    ArgumentStateMap.filter(
      argumentSlotOffset,
      morsel,
      (argumentRowId, nRows) =>
        createState(controllers.get(argumentRowId).state, nRows),
      predicate
      )
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

  override def takeNextIfCompleted(): STATE = {
    nextIfCompletedOrNull((state, isCompleted) => if (isCompleted) state else null.asInstanceOf[STATE])
  }

  override def takeNextIfCompletedOrElsePeek(): ArgumentStateWithCompleted[STATE] = {
    nextIfCompletedOrNull((state, isCompleted) => ArgumentStateWithCompleted(state, isCompleted))
  }

  private def nextIfCompletedOrNull[T](stateMapper: (STATE, Boolean) => T): T = {
    val controller = controllers.get(lastCompletedArgumentId + 1)
    if (controller != null) {
      if (controller.tryTake()) {
        lastCompletedArgumentId += 1
        controllers.remove(controller.state.argumentRowId)
        DebugSupport.ASM.log("ASM %s take %03d", argumentStateMapId, controller.state.argumentRowId)
        stateMapper(controller.state, true)
      } else {
        stateMapper(controller.state, false)
      }
    } else {
      null.asInstanceOf[T]
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

  override def initiate(argument: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long], initialCount: Int): Unit = {
    DebugSupport.ASM.log("ASM %s init %03d", argumentStateMapId, argument)
    val newController = newStateController(argument, argumentMorsel, argumentRowIdsForReducers, initialCount)
    val previousValue = controllers.put(argument, newController)
    Preconditions.checkState(previousValue == null, "ArgumentStateMap cannot re-initiate the same argument (argument: %d)", Long.box(argument))
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

  /**
   * A state controller that does not allow any mutating.
   *
   * This controller serves the use case when an argument state is constructed and immediately ready for consumption.
   * This is the case for, e.g. cartesian product, distinct, and limit. This `increment` and `decrement` will throw exceptions.
   *
   * `tryTake` and `take` are also forbidden, since the pattern for this controller uses [[ArgumentStateMap.peekCompleted()]].
   */
  class ImmutableStateController[STATE <: ArgumentState](override val state: STATE)
    extends AbstractArgumentStateMap.StateController[STATE] {

    override def increment(): Long = throw new IllegalStateException(s"Cannot mutate ${this.getClass.getSimpleName}")

    override def decrement(): Long = throw new IllegalStateException(s"Cannot mutate ${this.getClass.getSimpleName}")

    override def tryTake(): Boolean = throw new IllegalStateException(s"Cannot mutate ${this.getClass.getSimpleName}")

    override def take(): Boolean = throw new IllegalStateException(s"Cannot mutate ${this.getClass.getSimpleName}")

    override def isZero: Boolean = true

    override def toString: String = {
      s"[immutable, state: $state]"
    }
  }
}
