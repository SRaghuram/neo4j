/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.ExecutionState
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.{Buffer, SingletonBuffer}
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.{MemoryTracker, QueryContext, WithHeapUsageEstimation}
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Factory for all the basic state management components of the [[ExecutionState]].
  * The reason to not instantiate them directly is so that we can use either thread-safe or non-thread-safe
  * versions, depending on whether the execution is concurrent or not.
  */
trait StateFactory {
  def newBuffer[T <: WithHeapUsageEstimation](): Buffer[T]
  def newSingletonBuffer[T <: AnyRef](): SingletonBuffer[T]
  def newTracker(subscriber: QuerySubscriber,
                 queryContext: QueryContext,
                 tracer: QueryExecutionTracer): QueryCompletionTracker
  def newIdAllocator(): IdAllocator
  def newLock(id: String): Lock
  def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                              argumentSlotOffset: Int,
                                              factory: ArgumentStateFactory[S]): ArgumentStateMap[S]

  /**
    * Obtain the memory tracker (this call does not create a new object).
    */
  def memoryTracker: MemoryTracker
}
