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
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardCompletedStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardArgumentStateMap.StandardStateController
import org.neo4j.memory.MemoryTracker

/**
 * Not thread-safe of SingletonArgumentStateMap.
 */
class StandardSingletonArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                factory: ArgumentStateFactory[STATE])
  extends AbstractSingletonArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]] {

  override protected var controller: AbstractArgumentStateMap.StateController[STATE] = _

  override protected var hasController = true

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            initialCount: Int,
                                            memoryTracker: MemoryTracker): AbstractArgumentStateMap.StateController[STATE] = {
    if (factory.completeOnConstruction) {
      new StandardCompletedStateController(factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers, memoryTracker))
    } else {
      new StandardStateController(factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers, memoryTracker), initialCount)
    }
  }
}
