/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.StaticField

/**
  * Compiles [[IntermediateExpression]] into compiled byte code.
  */
class CachingExpressionCompilerBack(inner: DefaultExpressionCompilerBack,
                                    tracer: CachingExpressionCompilerTracer,
                                    cache: CachingExpressionCompilerCache
                                   ) extends ExpressionCompilerBack {

  override def compileExpression(e: IntermediateExpression): CompiledExpression = {
    if (onlyStaticFields(e)) {
      cache.expressionsCache.computeIfAbsent(e, innerCompileExpression(e))
    } else {
      innerCompileExpression(e)
    }
  }

  private def innerCompileExpression(e: IntermediateExpression) = {
    tracer.onCompileExpression()
    inner.compileExpression(e)
  }

  override def compileProjection(projection: IntermediateExpression): CompiledProjection = {
    if (onlyStaticFields(projection)) {
      cache.projectionsCache.computeIfAbsent(projection, innerCompilerProjection(projection))
    } else {
      innerCompilerProjection(projection)
    }
  }

  private def innerCompilerProjection(projection: IntermediateExpression) = {
    tracer.onCompileProjection()
    inner.compileProjection(projection)
  }

  override def compileGrouping(grouping: IntermediateGroupingExpression): CompiledGroupingExpression = {
    if (onlyStaticFields(grouping.computeKey) &&
        onlyStaticFields(grouping.getKey) &&
        onlyStaticFields(grouping.projectKey)) {
      cache.groupingsCache.computeIfAbsent(grouping, innerCompileGrouping(grouping))
    } else {
      innerCompileGrouping(grouping)
    }
  }

  private def innerCompileGrouping(grouping: IntermediateGroupingExpression) = {
    tracer.onCompileGrouping()
    inner.compileGrouping(grouping)
  }

  private def onlyStaticFields(e: IntermediateExpression): Boolean = {
    e.fields.forall(_.isInstanceOf[StaticField])
  }
}




