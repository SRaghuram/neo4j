/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.MemoizingMeasurable
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.SingletonBuffer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.MemoryTracker

/**
 * Factory for all the basic state management components of the [[ExecutionState]].
 * The reason to not instantiate them directly is so that we can use either thread-safe or non-thread-safe
 * versions, depending on whether the execution is concurrent or not.
 */
trait StateFactory {
  def newBuffer[T <: MemoizingMeasurable](operatorId: Id): Buffer[T]
  def newSingletonBuffer[T <: AnyRef](): SingletonBuffer[T]
  def newTracker(subscriber: QuerySubscriber,
                 queryContext: QueryContext,
                 tracer: QueryExecutionTracer): QueryCompletionTracker
  def newIdAllocator(): IdAllocator
  def newLock(id: String): Lock
  def newLowMark(startValue: Int): LowMark

  /**
   *
   * @param orderPreservingInParallel order is always preserved in single threaded. In parallel we decide if the argument state map preserves order based on this parameter.
   */
  def newArgumentStateMap[S <: ArgumentState](argumentStateMapId: ArgumentStateMapId,
                                              argumentSlotOffset: Int,
                                              factory: ArgumentStateFactory[S],
                                              orderPreservingInParallel: Boolean): ArgumentStateMap[S]

  /**
   * Obtain the query memory tracker (this call does not create a new object).
   */
  def memoryTracker: QueryMemoryTracker

  /**
   * Obtain the memory tracker (this call may create a new object or return an already existing one).
   */
  def newMemoryTracker(operatorId: Int): MemoryTracker
}
