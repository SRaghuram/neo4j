/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

/**
  * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
  */
class ConcurrentArgumentStateMap[S <: ArgumentState](val owningPlanId: Id,
                                                     val argumentSlotOffset: Int,
                                                     factory: ArgumentStateFactory[S]) extends ArgumentStateMap[S] {

  private val controllers = new java.util.concurrent.ConcurrentHashMap[Long, StateController[S]]()

  override def update(morsel: MorselExecutionContext,
                      onState: (S, MorselExecutionContext) => Unit): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argumentRowId, morselView) => {
        val controller = controllers.get(argumentRowId)
        controller.update(morselView, onState)
      }
    )
  }

  override def filter[U](readingRow: MorselExecutionContext,
                         onArgument: (S, Long) => U,
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

  override def takeCompleted(): Iterable[S] = {
    val completeStates = new ArrayBuffer[S]
    controllers.forEach((argument: Long, controller: StateController[S]) => {
      if (controller.take) {
        completeStates += controller.state
      }
    })
    completeStates.foreach(acc => controllers.remove(acc.argumentRowId))
    completeStates
  }

  override def hasCompleted: Boolean = {
    val iterator = controllers.values().iterator()

    while(iterator.hasNext) {
      val controller: StateController[S] = iterator.next()
      if (controller.isZero)
        return true
    }
    false
  }

  override def initiate(argument: Long): Unit = {
    val id = if (Zombie.DEBUG) s"ArgumentState[plan=$owningPlanId, rowId=$argument]" else "ArgumentState[...]"
    val newController = new StateController(id, factory.newArgumentState(argument))
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
  * Controller which knows when a [[ArgumentState]] has is complete,
  * and protects it from concurrent access.
  */
class StateController[STATE <: ArgumentState](id: String, val state: STATE) {
  private val count = new AtomicLong(1)
  private val lock = new ConcurrentLock(id)

  def increment(): Long = count.incrementAndGet()
  def decrement(): Long = count.decrementAndGet()
  def take: Boolean = count.compareAndSet(0, -1000000)
  def isZero: Boolean = count.get() == 0

  def update(morsel: MorselExecutionContext, onState: (STATE, MorselExecutionContext) => Unit): Unit = {
    lock.lock()
    onState(state, morsel)
    lock.unlock()
  }

  def compute[U](nRows: Long, onArgument: (STATE, Long) => U): U = {
    lock.lock()
    val u = onArgument(state, nRows)
    lock.unlock()
    u
  }
}
