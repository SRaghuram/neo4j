/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration


/**
  * Compiles a Cypher Expression to a class or intermediate representation
  */
trait ExpressionCompiler {

  /**
    * Compiles the given expression to an instance of [[CompiledExpression]]
    * @param e the expression to compile
    * @return an instance of [[CompiledExpression]] corresponding to the provided expression
    */
  def compileExpression(e: Expression): Option[CompiledExpression]

  /**
    * Compiles the given projection to an instance of [[CompiledProjection]]
    * @param projections the projection to compile
    * @return an instance of [[CompiledProjection]] corresponding to the provided projection
    */
  def compileProjection(projections: Map[String, Expression]): Option[CompiledProjection]

  /**
    * Compiles the given groupings to an instance of [[CompiledGroupingExpression]]
    * @param orderedGroupings the groupings to compile
    * @return an instance of [[CompiledGroupingExpression]] corresponding to the provided groupings
    */
  def compileGrouping(orderedGroupings: SlotConfiguration => Seq[(String, Expression, Boolean)]): Option[CompiledGroupingExpression]
}

object ExpressionCompiler {
  def defaultGenerator(slots: SlotConfiguration,
                       readOnly: Boolean,
                       codeGenerationMode: CodeGeneration.CodeGenerationMode,
                       namer: VariableNamer = new VariableNamer
                      ): ExpressionCompiler = new DefaultExpressionCompiler(slots, readOnly, codeGenerationMode, namer)
}
