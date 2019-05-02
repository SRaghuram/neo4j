/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{ConcurrentDemandControlSubscription, DemandControlSubscription}
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.{Buffer, ConcurrentBuffer}
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Implementation of [[StateFactory]] which constructs concurrent state management classes.
  */
object ConcurrentStateFactory extends StateFactory {
  override def newBuffer[T <: AnyRef](): Buffer[T] = new ConcurrentBuffer[T]

  override def newTracker(subscriber: QuerySubscriber,
                          demandControlSubscription: DemandControlSubscription,
                          queryContext: QueryContext): QueryCompletionTracker =
    new ConcurrentQueryCompletionTracker(subscriber, demandControlSubscription, queryContext)

  override def newIdAllocator(): IdAllocator = new ConcurrentIdAllocator

  override def newLock(id: String): Lock = new ConcurrentLock(id)

  override def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                       argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    new ConcurrentArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
  }

  override def newDemandControlSubscription: DemandControlSubscription = new ConcurrentDemandControlSubscription
}
