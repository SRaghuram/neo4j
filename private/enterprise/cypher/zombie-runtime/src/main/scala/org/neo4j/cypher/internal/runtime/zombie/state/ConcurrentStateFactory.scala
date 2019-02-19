/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, MorselAccumulator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Implementation of [[StateFactory]] which produces concurrent state management classes.
  */
object ConcurrentStateFactory extends StateFactory {
  override def newBuffer[T <: AnyRef](): Buffer[T] = new ConcurrentBuffer[T]

  override def newTracker(): Tracker = new ConcurrentTracker

  override def newIdAllocator(): IdAllocator = new ConcurrentIdAllocator

  override def newLock(id: Int): Lock = new ConcurrentLock(id)

  override def newArgumentStateMap[T <: MorselAccumulator](reducePlanId: Id,
                                                           argumentSlotOffset: Int,
                                                           constructor: () => T): ArgumentStateMap[T] = {
    new ConcurrentArgumentStateMap[T](reducePlanId, argumentSlotOffset, constructor)
  }
}
