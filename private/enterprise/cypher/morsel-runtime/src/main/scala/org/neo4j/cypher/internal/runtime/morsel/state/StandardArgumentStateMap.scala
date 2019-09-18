/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.StandardArgumentStateMap.StandardStateController

/**
  * Not thread-safe and quite naive implementation of ArgumentStateMap. JustGetItWorking(tm)
  */
class StandardArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                       val argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[STATE])
  extends AbstractArgumentStateMap[STATE, StandardStateController[STATE]] {

  override protected val controllers = new java.util.HashMap[Long, StandardStateController[STATE]]()

  override protected var lastCompletedArgumentId: Long = -1

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselExecutionContext,
                                            argumentRowIdsForReducers: Array[Long]): StandardStateController[STATE] =
    new StandardStateController(factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers),
                                factory.completeOnConstruction)
}

object StandardArgumentStateMap {

 /**
  * Controller which knows when an [[ArgumentState]] is complete.
  */
  private[state] class StandardStateController[STATE <: ArgumentState](override val state: STATE, completeOnConstruction: Boolean)
    extends AbstractArgumentStateMap.StateController[STATE] {

    private var _count: Long = if (completeOnConstruction) 0 else 1

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
