/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, MorselAccumulator, MorselAccumulatorFactory}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable

/**
  * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
  */
class StandardArgumentStateMap[T <: MorselAccumulator](val owningPlanId: Id,
                                                       val argumentSlotOffset: Int,
                                                       factory: MorselAccumulatorFactory[T]) extends ArgumentStateMap[T] {
  private val controllers = mutable.Map[Long, Controller]()

  override def update(morsel: MorselExecutionContext): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argument, morselView) => {
        val controller = controllers(argument)
        controller.accumulator.update(morselView)
      }
    )
  }

  override def takeCompleted(): Iterable[T] = {
    val complete = controllers.values.filter(_.count == 0)
    complete.foreach(controller => controllers -= controller.accumulator.argumentRowId)
    complete.map(_.accumulator)
  }

  override def hasCompleted: Boolean = {
    controllers.values.exists(_.count == 0)
  }

  override def initiate(argument: Long): Unit = {
    controllers += argument -> new Controller(factory.newAccumulator(argument))
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

  private class Controller(val accumulator: T) {
    var count: Long = 1
  }
}
