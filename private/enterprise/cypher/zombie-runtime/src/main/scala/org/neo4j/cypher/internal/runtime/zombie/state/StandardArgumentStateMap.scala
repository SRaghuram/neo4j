/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentState, ArgumentStateFactory, ArgumentStateMap}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable

/**
  * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
  */
class StandardArgumentStateMap[STATE <: ArgumentState](val owningPlanId: Id,
                                                       val argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[STATE]) extends ArgumentStateMap[STATE] {
  private val controllers = mutable.Map[Long, Controller]()

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
                                        isCancelled: STATE => Boolean): Seq[Long] = {
    ArgumentStateMap.filterCancelledArguments(argumentSlotOffset,
                                              morsel,
                                              argumentRowId => isCancelled(controllers(argumentRowId).state))
  }

  override def takeCompleted(): Iterable[STATE] = {
    val complete = controllers.values.filter(_.count == 0)
    complete.foreach(controller => controllers -= controller.state.argumentRowId)
    complete.map(_.state)
  }

  override def hasCompleted: Boolean = {
    controllers.values.exists(_.count == 0)
  }

  override def initiate(argument: Long): Unit = {
    controllers += argument -> new Controller(factory.newArgumentState(argument))
  }

  override def increment(argument: Long): Unit = {
    val controller = controllers(argument)
    controller.count += 1
    debug("incr %03d to %s".format(argument, controller.count))
  }

  override def decrement(argument: Long): Unit = {
    val controller = controllers(argument)
    if (controller.count == 0)
      throw new IllegalStateException("Cannot have negative reference counts!")
    controller.count -= 1
    debug("decr %03d to %s".format(argument, controller.count))
  }

  private class Controller(val state: STATE) {
    var count: Long = 1
  }
}
