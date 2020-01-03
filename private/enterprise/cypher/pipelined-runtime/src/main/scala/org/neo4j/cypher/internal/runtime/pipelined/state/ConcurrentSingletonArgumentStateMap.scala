/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.state.AbstractArgumentStateMap.ImmutableStateController
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentArgumentStateMap.ConcurrentStateController

/**
  * Thread-safe implementation of SingletonArgumentStateMap.
  */
class ConcurrentSingletonArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                  factory: ArgumentStateFactory[STATE])
  extends AbstractSingletonArgumentStateMap[STATE, AbstractArgumentStateMap.StateController[STATE]] {

  @volatile
  override protected var controller: AbstractArgumentStateMap.StateController[STATE] = _

  @volatile
  override protected var hasController = true

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
