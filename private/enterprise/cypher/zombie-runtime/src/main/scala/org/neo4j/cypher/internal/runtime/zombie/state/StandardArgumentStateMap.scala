/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable

/**
  * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
  */
class StandardArgumentStateMap[T <: MorselAccumulator](val owningPlanId: Id,
                                                       val argumentSlotOffset: Int,
                                                       constructor: () => T) extends ArgumentStateMap[T] {
  private val arguments = mutable.Map[Long, T]()
  private val counters = mutable.Map[Long, Long]().withDefaultValue(0)

  override def updateAndConsume(morsel: MorselExecutionContext): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argument, morselView) => {
        val accumulator = arguments.getOrElseUpdate(argument, constructor())
        accumulator.update(morselView)
        decrement(argument)
      }
    )
    morsel.removeCounter(owningPlanId)
  }

  override def consumeCompleted(): Iterator[T] = {
    val complete =
      for {
        argument <- counters.keys
        if counters(argument) == 0
      } yield argument

    complete.foreach(argument => counters -= argument)
    complete.map(arguments).toIterator
  }

  override def increment(argument: Long): Unit = {
    counters(argument) += 1
    debug("incr %03d to %s".format(argument, counters(argument)))
  }

  override def decrement(argument: Long): Unit = {
    val curr = counters(argument)
    if (curr == 0)
      throw new IllegalStateException("Cannot have negative reference counts!")
    counters(argument) = curr - 1
    debug("decr %03d to %s".format(argument, counters(argument)))
  }
}
