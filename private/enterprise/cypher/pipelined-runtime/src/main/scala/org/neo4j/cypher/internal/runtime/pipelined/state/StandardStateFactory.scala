/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.runtime.BoundedMemoryTracker
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MemoryTrackingStandardBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.SingletonBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.StandardBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.StandardSingletonBuffer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryTracker

/**
 * Implementation of [[StateFactory]] which creates not thread-safe implementation of the state management classes.
 */
class StandardStateFactory extends StateFactory {
  override def newBuffer[T <: Measurable](operatorId: Id): Buffer[T] = new StandardBuffer[T]

  override def newSingletonBuffer[T <: AnyRef](): SingletonBuffer[T] = new StandardSingletonBuffer[T]

  override def newTracker(subscriber: QuerySubscriber,
                          queryContext: QueryContext,
                          tracer: QueryExecutionTracer): QueryCompletionTracker =
    new StandardQueryCompletionTracker(subscriber, queryContext, tracer)

  override def newIdAllocator(): IdAllocator = new StandardIdAllocator

  override def newLock(id: String): Lock = new NoLock(id)

  override def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                                       argumentSlotOffset: Int,
                                                       factory: ArgumentStateFactory[S],
                                                       ordered: Boolean): ArgumentStateMap[S] = {
    if (argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) {
      new StandardSingletonArgumentStateMap[S](argumentStateMapId, factory)
    } else {
      new StandardArgumentStateMap[S](argumentStateMapId, argumentSlotOffset, factory)
    }
  }

  override def newMemoryTracker(operatorId: Int): MemoryTracker = EmptyMemoryTracker.INSTANCE

  override val memoryTracker: QueryMemoryTracker = NoMemoryTracker
}

class MemoryTrackingStandardStateFactory(transactionMemoryTracker: MemoryTracker) extends StandardStateFactory {
  override val memoryTracker: QueryMemoryTracker = BoundedMemoryTracker(transactionMemoryTracker)

  override def newBuffer[T <: Measurable](operatorId: Id): Buffer[T] = {
    val operatorMemoryTracker = memoryTracker.memoryTrackerForOperator(operatorId.x)
    new MemoryTrackingStandardBuffer[T](operatorMemoryTracker)
  }

  override def newMemoryTracker(operatorId: Int): MemoryTracker = {
    memoryTracker.memoryTrackerForOperator(operatorId)
  }
}
