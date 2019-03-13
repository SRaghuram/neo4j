/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, MorselAccumulator, MorselAccumulatorFactory}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Implementation of [[StateFactory]] which constructs concurrent state management classes.
  */
object ConcurrentStateFactory extends StateFactory {
  override def newBuffer[T <: AnyRef](): Buffer[T] = new ConcurrentBuffer[T]

  override def newTracker(): QueryCompletionTracker = new ConcurrentQueryCompletionTracker

  override def newIdAllocator(): IdAllocator = new ConcurrentIdAllocator

  override def newLock(id: String): Lock = new ConcurrentLock(id)

  override def newArgumentStateMap[ACC <: MorselAccumulator](reducePlanId: Id,
                                                           argumentSlotOffset: Int,
                                                           factory: MorselAccumulatorFactory[ACC]): ArgumentStateMap[ACC] = {
    new ConcurrentArgumentStateMap[ACC](reducePlanId, argumentSlotOffset, factory)
  }
}
