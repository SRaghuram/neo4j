/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.util.attribution.Id

/**
  * Expression compiler which performs full compilation from ast to compiled byte-code.
  */
class StandaloneExpressionCompiler(front: AbstractExpressionCompilerFront, back: ExpressionCompilerBack) {

  /**
    * Compiles the given expression to an instance of [[CompiledExpression]]
    * @param e the expression to compile
    * @return an instance of [[CompiledExpression]] corresponding to the provided expression
    */
  def compileExpression(e: Expression, id: Id): Option[CompiledExpression] = {
    front.compileExpression(e, id).map(back.compileExpression)
  }

  /**
    * Compiles the given projection to an instance of [[CompiledProjection]]
    * @param projections the projection to compile
    * @return an instance of [[CompiledProjection]] corresponding to the provided projection
    */
  def compileProjection(projections: Map[String, Expression], id: Id): Option[CompiledProjection] = {
    front.compileProjection(projections, id).map(back.compileProjection)
  }

  /**
    * Compiles the given groupings to an instance of [[CompiledGroupingExpression]]
    * @param orderedGroupings the groupings to compile
    * @return an instance of [[CompiledGroupingExpression]] corresponding to the provided groupings
    */
  def compileGrouping(orderedGroupings:  SlotConfiguration => Seq[(String, Expression, Boolean)], id: Id): Option[CompiledGroupingExpression] = {
    val orderedGroupingsBySlots = orderedGroupings(front.slots) // Apply the slot configuration to get the complete order
    val compiled = for {(k, v, _) <- orderedGroupingsBySlots
                        c <- front.compileExpression(v, id)} yield front.slots(k) -> c
    if (compiled.size < orderedGroupingsBySlots.size) None
    else {
      val grouping = front.compileGroupingExpression(compiled, ExpressionCompilation.GROUPING_KEY_NAME, id)
      Some(back.compileGrouping(grouping))
    }
  }
}

object StandaloneExpressionCompiler {
  def default(slots: SlotConfiguration,
              readOnly: Boolean,
              codeGenerationMode: CodeGeneration.CodeGenerationMode,
              compiledExpressionsContext: CompiledExpressionContext
             ): StandaloneExpressionCompiler = {
    val front = new DefaultExpressionCompilerFront(slots, readOnly, new VariableNamer)
    withFront(front, codeGenerationMode, compiledExpressionsContext)
  }

  def withFront(front: AbstractExpressionCompilerFront,
                codeGenerationMode: CodeGeneration.CodeGenerationMode,
                compiledExpressionsContext: CompiledExpressionContext
               ): StandaloneExpressionCompiler = {
    val back = {
      val defaultBack = new DefaultExpressionCompilerBack(codeGenerationMode)
      if (codeGenerationMode.saver.options.isEmpty) {
        new CachingExpressionCompilerBack(defaultBack, compiledExpressionsContext.cachingExpressionCompilerTracer,
          compiledExpressionsContext.cachingExpressionCompilerCache)
      } else {
        defaultBack
      }
    }
    new StandaloneExpressionCompiler(front, back)
  }

  def codeChain(slots: SlotConfiguration,
                readOnly: Boolean,
                codeGenerationMode: CodeGeneration.CodeGenerationMode,
                compiledExpressionsContext: CompiledExpressionContext,
                fallback: AbstractExpressionCompilerFront): StandaloneExpressionCompiler = {
    val front = new CodeChainExpressionCompiler(
      slots, new VariableNamer) {
      override def compileExpression(expression: Expression): Option[IntermediateExpression] = {
        try {
          super.compileExpression(expression)
        } catch {
          case _: MatchError => fallback.compileExpression(expression)
          case _: NotImplementedError => fallback.compileExpression(expression)
        }
      }
    }

    withFront(front, codeGenerationMode, compiledExpressionsContext)
  }
}
