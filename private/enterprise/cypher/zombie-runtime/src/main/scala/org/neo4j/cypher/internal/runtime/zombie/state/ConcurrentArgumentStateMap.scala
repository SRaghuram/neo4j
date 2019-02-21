/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, MorselAccumulator, Zombie}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

/**
  * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
  */
class ConcurrentArgumentStateMap[T <: MorselAccumulator](val owningPlanId: Id,
                                                         val argumentSlotOffset: Int,
                                                         constructor: () => T) extends ArgumentStateMap[T] {

  private val accumulatorControllers = new java.util.concurrent.ConcurrentHashMap[Long, AccumulatorController[T]]()

  override def update(morsel: MorselExecutionContext): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argumentRowId, morselView) => {
        val controller = accumulatorControllers.get(argumentRowId)
        controller.update(morselView)
        val newCount = controller.decrement()
        debug("decr %03d to %s".format(argumentRowId, newCount))
      }
    )
    morsel.removeCounter(owningPlanId)
  }

  override def consumeCompleted(): Iterator[T] = {
    val completeAccumulators = new ArrayBuffer[T]
    val completeArguments = new ArrayBuffer[Long]

    accumulatorControllers.forEach(new BiConsumer[Long, AccumulatorController[T]] {
      override def accept(argument: Long, controller: AccumulatorController[T]): Unit = {
        if (controller.isZero) {
          completeArguments += argument
          completeAccumulators += controller.result
        }
      }
    })

    completeArguments.foreach(arg => accumulatorControllers.remove(arg))
    completeAccumulators.toIterator
  }

  override def initiate(argument: Long): Unit = {
    val id = if (Zombie.DEBUG) s"Accumulator[plan=$owningPlanId, rowId=$argument]" else "Accumulator[...]"
    val newController = new AccumulatorController(id, constructor())
    accumulatorControllers.put(argument, newController)
  }

  override def increment(argument: Long): Unit = {
    val controller = accumulatorControllers.get(argument)
    val newCount = controller.increment()
    debug("incr %03d to %d".format(argument, newCount))
  }

  override def decrement(argument: Long): Unit = {
    val newCount = accumulatorControllers.get(argument).decrement()
    debug("decr %03d to %d".format(argument, newCount))
  }
}

/**
  * Controller which knows when a [[MorselAccumulator]] has completed accumulation,
  * and protects it from concurrent access.
  */
class AccumulatorController[T <: MorselAccumulator](id: String, accumulator: T) {
  private val count = new AtomicLong(1)
  private val lock = new ConcurrentLock(id)

  def increment(): Long = count.incrementAndGet()
  def decrement(): Long = count.decrementAndGet()
  def isZero: Boolean = count.compareAndSet(0, -1000000)

  def update(morsel: MorselExecutionContext): Unit = {
    lock.lock()
    accumulator.update(morsel)
    lock.unlock()
  }

  def result: T = accumulator
}
