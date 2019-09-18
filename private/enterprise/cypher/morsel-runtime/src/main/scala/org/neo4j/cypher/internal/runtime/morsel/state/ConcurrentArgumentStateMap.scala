/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ConcurrentArgumentStateMap.ConcurrentStateController

/**
  * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
  */
class ConcurrentArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                         val argumentSlotOffset: Int,
                                                         factory: ArgumentStateFactory[STATE])
  extends AbstractArgumentStateMap[STATE, ConcurrentStateController[STATE]] {

  override protected val controllers = new java.util.concurrent.ConcurrentHashMap[Long, ConcurrentStateController[STATE]]()

  @volatile
  override protected var lastCompletedArgumentId: Long = -1

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselExecutionContext,
                                            argumentRowIdsForReducers: Array[Long]): ConcurrentStateController[STATE] =
    new ConcurrentStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers), factory.completeOnConstruction)
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
  private[state] class ConcurrentStateController[STATE <: ArgumentState](override val state: STATE, completeOnConstruction: Boolean)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private val count = new AtomicLong(if (completeOnConstruction) 0 else 1)

    override def increment(): Long = count.incrementAndGet()

    override def decrement(): Long = count.decrementAndGet()

    /**
      * Tries to take a controller, if the count has reached zero.
      * @return `true` if this call took the controller, `false` if it was already taken or the count was not zero.
      */
    override def tryTake(): Boolean = count.compareAndSet(0, TAKEN)

    /**
      * Tries to take a controller.
      * @return `true` if this call took the controller, `false` if it was already taken.
      */
    override def take(): Boolean = count.getAndSet(TAKEN) >= 0

    override def isZero: Boolean = count.get() == 0

    override def toString: String = {
      s"[count: ${count.get()}, state: $state]"
    }
  }
}
