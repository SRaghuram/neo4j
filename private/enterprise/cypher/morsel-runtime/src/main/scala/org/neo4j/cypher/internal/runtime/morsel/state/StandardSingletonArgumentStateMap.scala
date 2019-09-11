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
  * Not thread-safe of SingletonArgumentStateMap.
  */
class StandardSingletonArgumentStateMap[STATE <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                factory: ArgumentStateFactory[STATE])
  extends AbstractSingletonArgumentStateMap[STATE, StandardStateController[STATE]] {

  override protected var controller: StandardStateController[STATE] = _

  override protected var hasController = true

  override protected var lastCompletedArgumentId: Long = -1

  override protected def newStateController(argument: Long,
                                            argumentMorsel: MorselExecutionContext,
                                            argumentRowIdsForReducers: Array[Long]): StandardStateController[STATE] =
    new StandardStateController(factory.newStandardArgumentState(argument, argumentMorsel, argumentRowIdsForReducers))
}
