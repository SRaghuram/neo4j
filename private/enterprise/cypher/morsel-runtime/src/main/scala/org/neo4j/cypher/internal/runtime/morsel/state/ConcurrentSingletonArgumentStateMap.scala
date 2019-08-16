/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ConcurrentArgumentStateMap.ConcurrentStateController

/**
  * Thread-safe implementation of SingletonArgumentStateMap.
  */
class ConcurrentSingletonArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                  factory: ArgumentStateFactory[STATE])
  extends AbstractSingletonArgumentStateMap[STATE, ConcurrentStateController[STATE]] {

  @volatile
  override protected var controller: ConcurrentStateController[STATE] = _

  @volatile
  override protected var lastCompletedArgumentId: Long = -1

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselExecutionContext,
                                            argumentRowIdsForReducers: Array[Long]): ConcurrentStateController[STATE] =
    new ConcurrentStateController(factory.newConcurrentArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
}
