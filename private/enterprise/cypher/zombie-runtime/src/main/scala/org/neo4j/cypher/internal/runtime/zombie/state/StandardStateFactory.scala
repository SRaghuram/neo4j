/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.{Buffer, StandardBuffer}

/**
  * Implementation of [[StateFactory]] which creates not thread-safe implementation of the state management classes.
  */
object StandardStateFactory extends StateFactory {
  override def newBuffer[T <: AnyRef](): Buffer[T] = new StandardBuffer[T]

  override def newTracker(): QueryCompletionTracker = new StandardQueryCompletionTracker

  override def newIdAllocator(): IdAllocator = new StandardIdAllocator

  override def newLock(id: String): Lock = new NoLock(id)

  override def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                       argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    new StandardArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
  }
}
