/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentCompletedStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentStateController
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/**
 * Thread-safe implementation of SingletonArgumentStateMap.
 */
class ConcurrentSingletonArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                  factory: ArgumentStateFactory[STATE])
  extends AbstractSingletonArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]](EmptyMemoryTracker.INSTANCE) {

  @volatile
  override protected var controller: AbstractArgumentStateMap.StateController[STATE] = _

  @volatile
  override protected var hasController = true

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int,
                                            memoryTracker: MemoryTracker): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      ConcurrentCompletedStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
    } else {
      new ConcurrentStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers), initialCount)
    }
  }
}
