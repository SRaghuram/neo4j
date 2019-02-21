/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.zombie.{ArgumentStateMap, ExecutionState, MorselAccumulator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Factory for all the basic state management components of the [[ExecutionState]].
  */
trait StateFactory {
  def newBuffer[T <: AnyRef](): Buffer[T]
  def newTracker(): Tracker
  def newIdAllocator(): IdAllocator
  def newLock(id: String): Lock
  def newArgumentStateMap[T <: MorselAccumulator](reducePlanId: Id,
                                                  argumentSlotOffset: Int,
                                                  constructor: () => T): ArgumentStateMap[T]
}
