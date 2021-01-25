/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

/**
  * Compiles [[IntermediateExpression]] into compiled byte code.
  */
trait ExpressionCompilerBack {

  def compileExpression(expression: IntermediateExpression): CompiledExpression

  def compileProjection(projection: IntermediateExpression): CompiledProjection

  def compileGrouping(grouping: IntermediateGroupingExpression): CompiledGroupingExpression
}
