/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.MemoizingMeasurable
import org.neo4j.cypher.internal.runtime.NoOpQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ConcurrentBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ConcurrentSingletonBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.SingletonBuffer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/**
 * Implementation of [[StateFactory]] which constructs concurrent state management classes.
 */
class ConcurrentStateFactory extends StateFactory {
  override def newBuffer[T <: MemoizingMeasurable](operatorId: Id): Buffer[T] = new ConcurrentBuffer[T]

  override def newSingletonBuffer[T <: AnyRef](): SingletonBuffer[T] = new ConcurrentSingletonBuffer[T]

  override def newTracker(subscriber: QuerySubscriber,
                          queryContext: QueryContext,
                          tracer: QueryExecutionTracer,
                          resouces: QueryResources): QueryCompletionTracker = {
    if (DebugSupport.DEBUG_TRACKER) {
      new ConcurrentDebugQueryCompletionTracker(subscriber, queryContext, tracer, resouces)
    } else {
      new ConcurrentQueryCompletionTracker(subscriber, queryContext, tracer, resouces)
    }
  }

  override def newIdAllocator(): IdAllocator = new ConcurrentIdAllocator

  override def newLock(id: String): Lock = new ConcurrentLock(id)

  override def newLowMark(startValue: Int): LowMark = new ConcurrentLowMark(startValue)

  override def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                       argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[S],
                                                       orderPreservingInParallel: Boolean,
                                                       memoryTracker: MemoryTracker,
                                                       morselSize: Int): ArgumentStateMap[S] = {
    if (argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) {
      new ConcurrentSingletonArgumentStateMap[S](argumentStateMapId, factory)
    } else if (orderPreservingInParallel) {
      new OrderedConcurrentArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
    } else {
      new ConcurrentArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
    }
  }

  // We currently don't track memory in parallel
  override val memoryTracker: QueryMemoryTracker = NoOpQueryMemoryTracker

  override def newMemoryTracker(operatorId: Int): MemoryTracker = EmptyMemoryTracker.INSTANCE
}
