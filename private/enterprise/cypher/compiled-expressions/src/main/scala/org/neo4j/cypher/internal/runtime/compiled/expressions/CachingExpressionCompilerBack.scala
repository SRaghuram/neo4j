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
    if (e.fields.isEmpty) {
      CachingExpressionCompilerBack.EXPRESSIONS.computeIfAbsent(e, innerCompileExpression(e))
    } else {
      innerCompileExpression(e)
    }
  }

  private def innerCompileExpression(e: IntermediateExpression) = {
    tracer.onCompileExpression()
    inner.compileExpression(e)
  }

  override def compileProjection(projection: IntermediateExpression): CompiledProjection = {
    if (projection.fields.isEmpty) {
      CachingExpressionCompilerBack.PROJECTIONS.computeIfAbsent(projection, innerCompilerProjection(projection))
    } else {
      innerCompilerProjection(projection)
    }
  }

  private def innerCompilerProjection(projection: IntermediateExpression) = {
    tracer.onCompileProjection()
    inner.compileProjection(projection)
  }

  override def compileGrouping(grouping: IntermediateGroupingExpression): CompiledGroupingExpression = {
    if (grouping.computeKey.fields.isEmpty &&
        grouping.getKey.fields.isEmpty &&
        grouping.projectKey.fields.isEmpty) {
      CachingExpressionCompilerBack.GROUPINGS.computeIfAbsent(grouping, innerCompileGrouping(grouping))
    } else {
      innerCompileGrouping(grouping)
    }
  }

  private def innerCompileGrouping(grouping: IntermediateGroupingExpression) = {
    tracer.onCompileGrouping()
    inner.compileGrouping(grouping)
  }
}

object CachingExpressionCompilerBack {
  val SIZE = 1000
  val EXPRESSIONS = new LFUCache[IntermediateExpression, CompiledExpression](SIZE)
  val PROJECTIONS = new LFUCache[IntermediateExpression, CompiledProjection](SIZE)
  val GROUPINGS = new LFUCache[IntermediateGroupingExpression, CompiledGroupingExpression](SIZE)
}


