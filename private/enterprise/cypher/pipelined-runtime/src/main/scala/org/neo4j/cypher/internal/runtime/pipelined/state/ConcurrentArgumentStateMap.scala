/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.state.AbstractArgumentStateMap.ImmutableStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentStateController

/**
  * Concurrent and quite naive implementation of ArgumentStateMap. Also JustGetItWorking(tm)
  */
class ConcurrentArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                         val argumentSlotOffset: Int,
                                                         factory: ArgumentStateFactory[STATE])
  extends AbstractArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]] {

  override protected val controllers = new java.util.concurrent.ConcurrentHashMap[Long, AbstractArgumentStateMap.StateController[STATE]]()

  @volatile
  override protected var lastCompletedArgumentId: Long = -1

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselExecutionContext,
                                            argumentRowIdsForReducers: Array[Long]): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      new ImmutableStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
    } else {
      new ConcurrentStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
    }
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
  private[state] class ConcurrentStateController[STATE <: ArgumentState](override val state: STATE)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private val count = new AtomicLong(1)

    override def increment(): Long = count.incrementAndGet()

    override def decrement(): Long = count.decrementAndGet()

    override def tryTake(): Boolean = count.compareAndSet(0, TAKEN)

    override def take(): Boolean = count.getAndSet(TAKEN) >= 0

    override def isZero: Boolean = count.get() == 0

    override def toString: String = {
      s"[count: ${count.get()}, state: $state]"
    }
  }
}
