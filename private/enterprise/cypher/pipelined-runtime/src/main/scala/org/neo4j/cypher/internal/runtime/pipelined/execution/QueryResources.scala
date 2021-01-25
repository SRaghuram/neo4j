/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

/**
 * Resources used by the pipelined runtime for query execution.
 * Each worker has its own resources and they are valid for multiple queries.
 */
class QueryResources(cursorFactory: CursorFactory,
                     cursorTracer: PageCursorTracer,
                     tracker: MemoryTracker,
                     val workerId: Int,
                     val nWorkers: Int,
                     val queryStatisticsTracker: MutableQueryStatistics = new MutableQueryStatistics) extends AutoCloseable {

  val expressionCursors: ExpressionCursors = new ExpressionCursors(cursorFactory, cursorTracer, tracker)
  val cursorPools: CursorPools = new CursorPools(cursorFactory, cursorTracer, tracker)
  private var _expressionVariables = new Array[AnyValue](8)
  private var _memoryTracker: MemoryTracker = EmptyMemoryTracker.INSTANCE

  // For correct profiling of dbHits in NestedPipeExpressions, when supported by a fallback to slotted pipes.
  var profileInformation: InterpretedProfileInformation = null

  def expressionVariables(nExpressionSlots: Int): Array[AnyValue] = {
    if (_expressionVariables.length < nExpressionSlots)
      _expressionVariables = new Array[AnyValue](nExpressionSlots)
    _expressionVariables
  }

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    expressionCursors.setKernelTracer(tracer)
    cursorPools.setKernelTracer(tracer)
  }

  def memoryTracker: MemoryTracker = _memoryTracker

  def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {
    _memoryTracker = memoryTracker
  }

  def resetMemoryTracker(): Unit = {
    _memoryTracker = EmptyMemoryTracker.INSTANCE
  }

  override def close(): Unit = {
    IOUtils.closeAll(expressionCursors, cursorPools)
  }
}

