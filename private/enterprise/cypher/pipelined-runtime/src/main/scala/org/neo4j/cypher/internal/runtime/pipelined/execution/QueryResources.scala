/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.internal.kernel.api.{CursorFactory, KernelReadTracer}
import org.neo4j.io.IOUtils
import org.neo4j.values.AnyValue

/**
  * Resources used by the pipelined runtime for query execution.
  * Each worker has its own resources and they are valid for multiple queries.
  */
class QueryResources(cursorFactory: CursorFactory) extends AutoCloseable {

  val expressionCursors: ExpressionCursors = new ExpressionCursors(cursorFactory)
  val cursorPools: CursorPools = new CursorPools(cursorFactory)
  private var _expressionVariables = new Array[AnyValue](8)

  def expressionVariables(nExpressionSlots: Int): Array[AnyValue] = {
    if (_expressionVariables.length < nExpressionSlots)
      _expressionVariables = new Array[AnyValue](nExpressionSlots)
    _expressionVariables
  }

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    expressionCursors.nodeCursor.setTracer(tracer)
    expressionCursors.relationshipScanCursor.setTracer(tracer)
    expressionCursors.propertyCursor.setTracer(tracer)
    cursorPools.setKernelTracer(tracer)
  }

  override def close(): Unit = {
    IOUtils.closeAll(expressionCursors, cursorPools)
  }
}

