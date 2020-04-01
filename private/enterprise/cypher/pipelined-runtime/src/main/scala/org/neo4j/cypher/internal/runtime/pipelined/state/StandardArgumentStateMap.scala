/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.AbstractArgumentStateMap.ImmutableStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardStateController

/**
 * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
 *
 * This is an ordered argument state map. Order is kept by using a `LinkedHashMap`.
 * No methods need to be overridden because scheduling guarantees that argument states
 * will be completed in argument row id order.
 *
 * There is no unordered standard argument state map because the performance overhead of a `LinkedHashMap` is negligible.
 */
class StandardArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                       val argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[STATE])
  extends AbstractArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]] {

  // Using a LinkedHashMap to get insertion iteration order, which will be argument-row-id order.
  override protected val controllers = new java.util.LinkedHashMap[Long, AbstractArgumentStateMap.StateController[STATE]]()

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      new ImmutableStateController(factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
    } else {
      new StandardStateController(factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers), initialCount)
    }
  }
}

object StandardArgumentStateMap {

  /**
   * Controller which knows when an [[ArgumentState]] is complete.
   */
  private[state] class StandardStateController[STATE <: ArgumentState](override val state: STATE, initialCount: Int)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private var _count: Long = initialCount

    override def isZero: Boolean = _count == 0

    override def increment(): Long = {
      _count += 1
      _count
    }

    override def decrement(): Long = {
      _count -= 1
      _count
    }

    // No actual "taking" in single threaded
    override def tryTake(): Boolean = isZero

    // No actual "taking" in single threaded
    override def take(): Boolean = true

    override def toString: String = {
      s"[count: ${_count}, state: $state]"
    }
  }
}
