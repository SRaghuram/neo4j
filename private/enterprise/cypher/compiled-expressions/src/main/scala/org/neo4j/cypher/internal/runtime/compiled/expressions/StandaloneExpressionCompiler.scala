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
  * Expression compiler which performs full compilation from ast to compiled byte-code.
  */
class StandaloneExpressionCompiler(front: AbstractExpressionCompilerFront, back: ExpressionCompilerBack) {

  /**
    * Compiles the given expression to an instance of [[CompiledExpression]]
    * @param e the expression to compile
    * @return an instance of [[CompiledExpression]] corresponding to the provided expression
    */
  def compileExpression(e: Expression): Option[CompiledExpression] = {
    front.compileExpression(e).map(back.compileExpression)
  }

  /**
    * Compiles the given projection to an instance of [[CompiledProjection]]
    * @param projections the projection to compile
    * @return an instance of [[CompiledProjection]] corresponding to the provided projection
    */
  def compileProjection(projections: Map[String, Expression]): Option[CompiledProjection] = {
    front.compileProjection(projections).map(back.compileProjection)
  }

  /**
    * Compiles the given groupings to an instance of [[CompiledGroupingExpression]]
    * @param orderedGroupings the groupings to compile
    * @return an instance of [[CompiledGroupingExpression]] corresponding to the provided groupings
    */
  def compileGrouping(orderedGroupings:  SlotConfiguration => Seq[(String, Expression, Boolean)]): Option[CompiledGroupingExpression] = {
    val orderedGroupingsBySlots = orderedGroupings(front.slots) // Apply the slot configuration to get the complete order
    val compiled = for {(k, v, _) <- orderedGroupingsBySlots
                        c <- front.compileExpression(v)} yield front.slots(k) -> c
    if (compiled.size < orderedGroupingsBySlots.size) None
    else {
      val grouping = front.compileGroupingExpression(compiled, "key")
      Some(back.compileGrouping(grouping))
    }
  }
}

object StandaloneExpressionCompiler {
  def default(slots: SlotConfiguration,
              readOnly: Boolean,
              codeGenerationMode: CodeGeneration.CodeGenerationMode
             ): StandaloneExpressionCompiler = {
    val front = new DefaultExpressionCompilerFront(slots, readOnly, new VariableNamer)
    val back = new ExpressionCompilerBack(codeGenerationMode)
    new StandaloneExpressionCompiler(front, back)
  }
}
