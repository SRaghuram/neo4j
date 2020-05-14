/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

/**
  * Traces expression compilation events.
  */
trait CachingExpressionCompilerTracer {

  def onCompileExpression(): Unit

  def onCompileProjection(): Unit

  def onCompileGrouping(): Unit
}

object CachingExpressionCompilerTracer {
  val NONE: CachingExpressionCompilerTracer =
    new CachingExpressionCompilerTracer {
      override def onCompileExpression(): Unit = {}
      override def onCompileProjection(): Unit = {}
      override def onCompileGrouping(): Unit = {}
    }
}
