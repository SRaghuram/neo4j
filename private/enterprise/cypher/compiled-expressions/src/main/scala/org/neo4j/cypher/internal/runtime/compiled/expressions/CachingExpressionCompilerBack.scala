/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.cache.LFUCache

/**
  * Compiles [[IntermediateExpression]] into compiled byte code.
  */
class CachingExpressionCompilerBack(inner: DefaultExpressionCompilerBack,
                                    tracer: CachingExpressionCompilerTracer
                                   ) extends ExpressionCompilerBack {

  override def compileExpression(e: IntermediateExpression): CompiledExpression = {
    CachingExpressionCompilerBack.EXPRESSIONS.computeIfAbsent(e, {
      tracer.onCompileExpression()
      inner.compileExpression(e)
    })
  }

  override def compileProjection(projection: IntermediateExpression): CompiledProjection = {
    CachingExpressionCompilerBack.PROJECTIONS.computeIfAbsent(projection, {
      tracer.onCompileProjection()
      inner.compileProjection(projection)
    })
  }

  override def compileGrouping(grouping: IntermediateGroupingExpression): CompiledGroupingExpression = {
    CachingExpressionCompilerBack.GROUPINGS.computeIfAbsent(grouping, {
      tracer.onCompileGrouping()
      inner.compileGrouping(grouping)
    })
  }
}

object CachingExpressionCompilerBack {
  val SIZE = 1000
  val EXPRESSIONS = new LFUCache[IntermediateExpression, CompiledExpression](SIZE)
  val PROJECTIONS = new LFUCache[IntermediateExpression, CompiledProjection](SIZE)
  val GROUPINGS = new LFUCache[IntermediateGroupingExpression, CompiledGroupingExpression](SIZE)
}


