/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, MorselAccumulator, MorselAccumulatorFactory, Zombie}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

/**
  * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
  */
class ConcurrentArgumentStateMap[ACC <: MorselAccumulator](val owningPlanId: Id,
                                                           val argumentSlotOffset: Int,
                                                           factory: MorselAccumulatorFactory[ACC]) extends ArgumentStateMap[ACC] {

  private val controllers = new java.util.concurrent.ConcurrentHashMap[Long, AccumulatorController[ACC]]()

  override def update(morsel: MorselExecutionContext): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argumentRowId, morselView) => {
        val controller = controllers.get(argumentRowId)
        controller.update(morselView)
      }
    )
  }

  override def filter[U](readingRow: MorselExecutionContext,
                         onArgument: (ACC, Long) => U,
                         onRow: (U, MorselExecutionContext) => Boolean): Unit = {
    ArgumentStateMap.filter(
      argumentSlotOffset,
      readingRow,
      (argumentRowId, nRows) => {
        controllers.get(argumentRowId).compute(nRows, onArgument)
      },
      onRow
    )
  }

  override def takeCompleted(): Iterable[ACC] = {
    val completeAccumulators = new ArrayBuffer[ACC]
    controllers.forEach((argument: Long, controller: AccumulatorController[ACC]) => {
      if (controller.take) {
        completeAccumulators += controller.accumulator
      }
    })
    completeAccumulators.foreach(acc => controllers.remove(acc.argumentRowId))
    completeAccumulators
  }

  override def hasCompleted: Boolean = {
    val iterator = controllers.values().iterator()

    while(iterator.hasNext) {
      val controller: AccumulatorController[ACC] = iterator.next()
      if (controller.isZero)
        return true
    }
    false
  }

  override def initiate(argument: Long): Unit = {
    val id = if (Zombie.DEBUG) s"Accumulator[plan=$owningPlanId, rowId=$argument]" else "Accumulator[...]"
    val newController = new AccumulatorController(id, factory.newAccumulator(argument))
    controllers.put(argument, newController)
  }

  override def increment(argument: Long): Unit = {
    val controller = controllers.get(argument)
    val newCount = controller.increment()
    debug("incr %03d to %d".format(argument, newCount))
  }

  override def decrement(argument: Long): Unit = {
    val newCount = controllers.get(argument).decrement()
    debug("decr %03d to %d".format(argument, newCount))
  }
}

/**
  * Controller which knows when a [[MorselAccumulator]] has completed accumulation,
  * and protects it from concurrent access.
  */
class AccumulatorController[ACC <: MorselAccumulator](id: String, val accumulator: ACC) {
  private val count = new AtomicLong(1)
  private val lock = new ConcurrentLock(id)

  def increment(): Long = count.incrementAndGet()
  def decrement(): Long = count.decrementAndGet()
  def take: Boolean = count.compareAndSet(0, -1000000)
  def isZero: Boolean = count.get() == 0

  def update(morsel: MorselExecutionContext): Unit = {
    lock.lock()
    accumulator.update(morsel)
    lock.unlock()
  }

  def compute[U](nRows: Long, onArgument: (ACC, Long) => U): U = {
    lock.lock()
    val u = onArgument(accumulator, nRows)
    lock.unlock()
    u
  }
}
