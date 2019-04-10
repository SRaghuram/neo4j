/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Buffer

/**
  * Factory for all the basic state management components of the [[ExecutionState]].
  * The reason to not instantiate them directly is so that we can use either thread-safe or non-thread-safe
  * versions, depending on whether the execution is concurrent or not.
  */
trait StateFactory {
  def newBuffer[T <: AnyRef](): Buffer[T]
  def newTracker(): QueryCompletionTracker
  def newIdAllocator(): IdAllocator
  def newLock(id: String): Lock
  def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                              argumentSlotOffset: Int,
                                              factory: ArgumentStateFactory[S]): ArgumentStateMap[S]
}
