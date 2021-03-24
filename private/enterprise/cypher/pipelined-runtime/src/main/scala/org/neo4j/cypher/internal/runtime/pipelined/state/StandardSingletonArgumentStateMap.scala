/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardCompletedStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardStateController
import org.neo4j.memory.MemoryTracker

/**
 * Not thread-safe of SingletonArgumentStateMap.
 */
class StandardSingletonArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                factory: ArgumentStateFactory[STATE],
                                                                memoryTracker: MemoryTracker)
  extends AbstractSingletonArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]](memoryTracker) {

  override protected var controller: AbstractArgumentStateMap.StateController[STATE] = _

  override protected var hasController = true

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int,
                                            memoryTracker: MemoryTracker,
                                            withPeekerTracking: Boolean): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      val state = factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers, memoryTracker)
      memoryTracker.allocateHeap(state.shallowSize)
      new StandardCompletedStateController(state)
    } else {
      val state = factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers, memoryTracker)
      memoryTracker.allocateHeap(state.shallowSize)
      new StandardStateController(state, initialCount)
    }
  }
}
