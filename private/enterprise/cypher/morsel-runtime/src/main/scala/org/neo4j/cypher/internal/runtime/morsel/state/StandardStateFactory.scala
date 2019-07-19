/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.{BoundedStandardBuffer, Buffer, SingletonBuffer, StandardBuffer, StandardSingletonBuffer}
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.{MemoryTracker, NoMemoryTracker, QueryContext, StandardMemoryTracker, WithHeapUsageEstimation}
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Implementation of [[StateFactory]] which creates not thread-safe implementation of the state management classes.
  */
class StandardStateFactory extends StateFactory {
  override def newBuffer[T <: WithHeapUsageEstimation](): Buffer[T] = new StandardBuffer[T]

  override def newSingletonBuffer[T <: AnyRef](): SingletonBuffer[T] = new StandardSingletonBuffer[T]

  override def newTracker(subscriber: QuerySubscriber,
                          queryContext: QueryContext,
                          tracer: QueryExecutionTracer): QueryCompletionTracker =
    new StandardQueryCompletionTracker(subscriber, queryContext, tracer)

  override def newIdAllocator(): IdAllocator = new StandardIdAllocator

  override def newLock(id: String): Lock = new NoLock(id)

  override def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                       argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[S]): ArgumentStateMap[S] = {
    new StandardArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
  }

  override val memoryTracker: MemoryTracker = NoMemoryTracker
}

class MemoryTrackingStandardStateFactory(transactionMaxMemory: Long) extends StandardStateFactory {
  override def newBuffer[T <: WithHeapUsageEstimation](): Buffer[T] = new BoundedStandardBuffer[T](transactionMaxMemory)

  override val memoryTracker: MemoryTracker = StandardMemoryTracker(transactionMaxMemory)
}