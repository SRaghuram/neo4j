/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.zombie.state.StandardArgumentStateMap.StandardStateController

import scala.collection.mutable

/**
  * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
  */
class StandardArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                       val argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[STATE]) extends ArgumentStateMap[STATE] {
  private val controllers = mutable.Map[Long, StandardStateController[STATE]]()

  override def update(morsel: MorselExecutionContext,
                      onState: (STATE, MorselExecutionContext) => Unit): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argument, morselView) => {
        val controller = controllers(argument)
        onState(controller.state, morselView)
      }
    )
  }

  override def filter[U](readingRow: MorselExecutionContext,
                         onArgument: (STATE, Long) => U,
                         onRow: (U, MorselExecutionContext) => Boolean): Unit = {
    ArgumentStateMap.filter(
      argumentSlotOffset,
      readingRow,
      (argumentRowId, nRows) =>
        onArgument(controllers(argumentRowId).state, nRows),
      onRow
    )
  }

  override def filterCancelledArguments(morsel: MorselExecutionContext,
                                        isCancelled: STATE => Boolean): IndexedSeq[Long] = {
    ArgumentStateMap.filterCancelledArguments(argumentSlotOffset,
                                              morsel,
                                              argumentRowId => isCancelled(controllers(argumentRowId).state))
  }

  override def takeOneCompleted(): STATE = {
    val iterator = controllers.values.iterator

    while(iterator.hasNext) {
      val controller = iterator.next()
      if (controller.isZero) {
        controllers -= controller.state.argumentRowId
        return controller.state
      }
    }
    null.asInstanceOf[STATE]
  }

  override def peekCompleted(): Iterator[STATE] = {
    controllers.values.iterator.filter(_.isZero).map(_.state)
  }

  override def peek(argumentId: Long): STATE = {
    val controller = controllers(argumentId)
    if (controller != null) {
      controller.state
    } else {
      null.asInstanceOf[STATE]
    }
  }

  override def hasCompleted: Boolean = {
    controllers.values.exists(_.isZero)
  }

  override def hasCompleted(argument: Long): Boolean = {
    controllers(argument).isZero
  }

  override def remove(argument: Long): Boolean = {
    controllers.remove(argument).isDefined
  }

  override def initiate(argument: Long): Unit = {
    controllers += argument -> new StandardStateController(factory.newStandardArgumentState(argument))
  }

  override def increment(argument: Long): Unit = {
    val controller = controllers(argument)
    controller.increment()
    debug("ASM %s incr %03d to %s".format(argumentStateMapId, argument, controller.count))
  }

  override def decrement(argument: Long): Boolean = {
    val controller = controllers(argument)
    if (controller.isZero) {
      throw new IllegalStateException("Cannot have negative reference counts!")
    }
    controller.decrement()
    debug("ASM %s decr %03d to %s".format(argumentStateMapId, argument, controller.count))
    controller.isZero
  }
}

object StandardArgumentStateMap {

 /**
  * Controller which knows when an [[ArgumentState]] is complete.
  */
  private[StandardArgumentStateMap] class StandardStateController[STATE <: ArgumentState](val state: STATE) {
    private var _count: Long = 1

    def isZero: Boolean = _count == 0

    def increment(): Unit = _count += 1

    def decrement(): Unit = _count -= 1

    def count: Long = _count
  }
}
