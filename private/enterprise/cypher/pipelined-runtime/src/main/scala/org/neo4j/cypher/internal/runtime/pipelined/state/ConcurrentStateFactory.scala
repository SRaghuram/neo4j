/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, TopLevelArgument}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.{Buffer, ConcurrentBuffer, ConcurrentSingletonBuffer, SingletonBuffer}
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext, QueryMemoryTracker, WithHeapUsageEstimation}
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Implementation of [[StateFactory]] which constructs concurrent state management classes.
  */
class ConcurrentStateFactory extends StateFactory {
  override def newBuffer[T <: WithHeapUsageEstimation](): Buffer[T] = new ConcurrentBuffer[T]

  override def newSingletonBuffer[T <: AnyRef](): SingletonBuffer[T] = new ConcurrentSingletonBuffer[T]

  override def newTracker(subscriber: QuerySubscriber,
                          queryContext: QueryContext,
                          tracer: QueryExecutionTracer): QueryCompletionTracker =
    new ConcurrentQueryCompletionTracker(subscriber, queryContext, tracer)

  override def newIdAllocator(): IdAllocator = new ConcurrentIdAllocator

  override def newLock(id: String): Lock = new ConcurrentLock(id)

  override def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                       argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    if (argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) {
      new ConcurrentSingletonArgumentStateMap[S](argumentStateMapId, factory)
    } else {
      new ConcurrentArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
    }
  }

  // We currently don't track memory in parallel
  override val memoryTracker: QueryMemoryTracker = NoMemoryTracker
}
