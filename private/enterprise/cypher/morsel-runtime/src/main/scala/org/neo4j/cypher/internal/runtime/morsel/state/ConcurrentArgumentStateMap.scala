/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ConcurrentArgumentStateMap.ConcurrentStateController

import scala.collection.JavaConverters._

/**
  * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
  */
class ConcurrentArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                         val argumentSlotOffset: Int,
                                                         factory: ArgumentStateFactory[STATE]) extends ArgumentStateMap[STATE] {

  private val controllers = new java.util.concurrent.ConcurrentHashMap[Long, ConcurrentStateController[STATE]]()

  override def update(morsel: MorselExecutionContext,
                      onState: (STATE, MorselExecutionContext) => Unit): Unit = {
    ArgumentStateMap.foreachArgument(
      argumentSlotOffset,
      morsel,
      (argumentRowId, morselView) => {
        val controller = controllers.get(argumentRowId)
        onState(controller.state, morselView)
      }
    )
  }

  def update(argumentRowId: Long, onState: STATE => Unit): Unit = {
    onState(controllers.get(argumentRowId).state)
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
      val controller: ConcurrentStateController[STATE] = iterator.next()
      if (controller.take) {
        controllers.remove(controller.state.argumentRowId)
        debug("ASM %s take %03d".format(argumentStateMapId, controller.state.argumentRowId))
        return controller.state
      }
    }
    null.asInstanceOf[STATE]
  }

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
      val controller: ConcurrentStateController[STATE] = iterator.next()
      if (controller.isZero)
        return true
    }
    false
  }

  override def hasCompleted(argument: Long): Boolean = {
    val controller = controllers.get(argument)
    controller != null && controller.isZero
  }

  override def remove(argument: Long): Boolean = {
    debug("ASM %s rem %03d".format(argumentStateMapId, argument))
    controllers.remove(argument) != null
  }

  override def initiate(argument: Long): Unit = {
    val id = if (DEBUG) s"ArgumentState[id=$argumentStateMapId, rowId=$argument]" else "ArgumentState[...]"
    debug("ASM %s init %03d".format(argumentStateMapId, argument))
    val newController = new ConcurrentStateController(id, factory.newConcurrentArgumentState(argument))
    controllers.put(argument, newController)
  }

  override def increment(argument: Long): Unit = {
    val controller = controllers.get(argument)
    val newCount = controller.increment()
    debug("ASM %s incr %03d to %d".format(argumentStateMapId, argument, newCount))
  }

  override def decrement(argument: Long): Boolean = {
    val newCount = controllers.get(argument).decrement()
    debug("ASM %s decr %03d to %d".format(argumentStateMapId, argument, newCount))
    newCount == 0
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb ++= "ConcurrentArgumentStateMap(\n"
    controllers.forEach((argumentRowId, controller) => {
      sb ++= s"$argumentRowId -> $controller\n"
    })
    sb += ')'
    sb.result()
  }
}


object ConcurrentArgumentStateMap {
  /**
    * CAS the count to this value once taken.
    */
  private val TAKEN = -1000000

  /**
    * Controller which knows when an [[ArgumentState]] is complete,
    * and protects it from concurrent access.
    */
  private[ConcurrentArgumentStateMap] class ConcurrentStateController[STATE <: ArgumentState](id: String, val state: STATE) {
    private val count = new AtomicLong(1)

    def increment(): Long = count.incrementAndGet()

    def decrement(): Long = count.decrementAndGet()

    def take: Boolean = count.compareAndSet(0, TAKEN)

    def isZero: Boolean = count.get() == 0

    override def toString: String = {
      s"[count: ${count.get()}, state: $state]"
    }
  }
}
