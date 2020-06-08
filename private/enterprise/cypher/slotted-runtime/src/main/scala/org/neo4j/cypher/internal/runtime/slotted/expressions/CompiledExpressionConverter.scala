/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode
import org.neo4j.cypher.internal.Assertion.assertionsEnabled
import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerTracer
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledGroupingExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledProjection
import org.neo4j.cypher.internal.runtime.compiled.expressions.StandaloneExpressionCompiler
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExtendedExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.RandFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter.COMPILE_LIMIT
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue

class CompiledExpressionConverter(log: Log,
                                  physicalPlan: PhysicalPlan,
                                  tokenContext: TokenContext,
                                  readOnly: Boolean,
                                  codeGenerationMode: CodeGenerationMode,
                                  cachingExpressionCompilerTracer: CachingExpressionCompilerTracer,
                                  neverFail: Boolean = false) extends ExpressionConverter {

  //uses an inner converter to simplify compliance with Expression trait
  private val inner = new ExpressionConverters(SlottedExpressionConverters(physicalPlan), CommunityExpressionConverter(tokenContext))

  override def toCommandExpression(id: Id, expression: expressions.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    //we don't deal with aggregations
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None

    case e if sizeOf(e) > COMPILE_LIMIT => try {
      log.debug(s"Compiling expression: $expression")
      StandaloneExpressionCompiler.default(physicalPlan.slotConfigurations(id), readOnly, codeGenerationMode, cachingExpressionCompilerTracer)
        .compileExpression(e)
        .map(CompileWrappingExpression(_, inner.toCommandExpression(id, expression)))
    } catch {
      case NonFatalCypherError(t) =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile expression: $e", t)
        None
    }

    case _ => None
  }



  private def sizeOf(expression: expressions.Expression)= expression.treeCount {
    case _: expressions.Expression => true
  }

  override def toCommandProjection(id: Id, projections: Map[String, expressions.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    try {
      val totalSize = projections.values.foldLeft(0)((acc, current) => acc + sizeOf(current))
      if (totalSize > COMPILE_LIMIT) {
        log.debug(s" Compiling projection: $projections")
        StandaloneExpressionCompiler.default(physicalPlan.slotConfigurations(id), readOnly, codeGenerationMode, cachingExpressionCompilerTracer)
          .compileProjection(projections)
          .map(CompileWrappingProjection(_, projections.isEmpty))
      } else None
    }
    catch {
      case NonFatalCypherError(t) =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile projection: $projections", t)
        None
    }
  }

  override def toGroupingExpression(id: Id,
                                    projections: Map[String, expressions.Expression],
                                    orderToLeverage: Seq[expressions.Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    try {
      if(orderToLeverage.nonEmpty) {
        // TODO Support compiled ordered GroupingExpression
        // UPDATE: In theory this should now be supported...
        None
      } else {
        val totalSize = projections.values.foldLeft(0)((acc, current) => acc + sizeOf(current))
        if (totalSize > COMPILE_LIMIT) {
          log.debug(s" Compiling grouping expression: $projections")
          StandaloneExpressionCompiler.default(physicalPlan.slotConfigurations(id), readOnly, codeGenerationMode, cachingExpressionCompilerTracer)
            .compileGrouping(orderGroupingKeyExpressions(projections, orderToLeverage))
            .map(CompileWrappingDistinctGroupingExpression(_, projections.isEmpty))
        } else None
      }
    }
    catch {
      case NonFatalCypherError(t) =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile grouping expression: $projections", t)
        None
    }
  }

  private def shouldThrow = !neverFail && assertionsEnabled()
}

object CompiledExpressionConverter {
  private val COMPILE_LIMIT: Int = 2
}

case class CompileWrappingDistinctGroupingExpression(grouping: CompiledGroupingExpression, isEmpty: Boolean) extends GroupingExpression {

  override type KeyType = AnyValue

  override def computeGroupingKey(context: ReadableRow, state: QueryState): AnyValue =
    grouping.computeGroupingKey(context, state.query, state.params, state.cursors, state.expressionVariables)

  override def computeOrderedGroupingKey(groupingKey: AnyValue): AnyValue =
    throw new IllegalStateException("Compiled expressions do not support this yet.")

  override def getGroupingKey(context: CypherRow): AnyValue = grouping.getGroupingKey(context)

  override def project(context: WritableRow, groupingKey: AnyValue): Unit =
    grouping.projectGroupingKey(context, groupingKey)
}

case class CompileWrappingProjection(projection: CompiledProjection, isEmpty: Boolean) extends CommandProjection {

  override def project(ctx: ReadWriteRow, state: QueryState): Unit =
    projection.project(ctx, state.query, state.params, state.cursors, state.expressionVariables)
}

case class CompileWrappingExpression(ce: CompiledExpression, legacy: Expression) extends ExtendedExpression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq(legacy)

  override def children: Seq[AstNode[_]] = Seq(legacy)

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    ce.evaluate(row, state.query, state.params, state.cursors, state.expressionVariables)

  override def toString: String = legacy.toString

  override val isDeterministic: Boolean = !legacy.exists {
    case RandFunction() => true
    case _              => false
  }
}

