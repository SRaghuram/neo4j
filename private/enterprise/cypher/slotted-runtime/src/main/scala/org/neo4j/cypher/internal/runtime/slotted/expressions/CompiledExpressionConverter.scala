/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode
import org.neo4j.cypher.internal.Assertion.assertionsEnabled
import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.defaultGenerator
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CompiledExpression, CompiledProjection, _}
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ExtendedExpression, RandFunction}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.v4_0.expressions.FunctionInvocation
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
import org.neo4j.exceptions.InternalException
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue

class CompiledExpressionConverter(log: Log,
                                  physicalPlan: PhysicalPlan,
                                  tokenContext: TokenContext,
                                  readOnly: Boolean,
                                  codeGenerationMode: CodeGenerationMode,
                                  neverFail: Boolean = false) extends ExpressionConverter {

  import CompiledExpressionConverter.COMPILE_LIMIT
  //uses an inner converter to simplify compliance with Expression trait
  private val inner = new ExpressionConverters(SlottedExpressionConverters(physicalPlan), CommunityExpressionConverter(tokenContext))

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    //we don't deal with aggregations
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None

    case e if sizeOf(e) > COMPILE_LIMIT => try {
      log.debug(s"Compiling expression: $expression")
      defaultGenerator(physicalPlan.slotConfigurations(id), readOnly, codeGenerationMode)
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


  import org.neo4j.cypher.internal.util.Foldable._

  private def sizeOf(expression: ast.Expression)= expression.treeCount {
    case _: ast.Expression => true
  }

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    try {
      val totalSize = projections.values.foldLeft(0)((acc, current) => acc + sizeOf(current))
      if (totalSize > COMPILE_LIMIT) {
        log.debug(s" Compiling projection: $projections")
        defaultGenerator(physicalPlan.slotConfigurations(id), readOnly, codeGenerationMode)
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
                                    projections: Map[String, ast.Expression],
                                    orderToLeverage: Seq[ast.Expression],
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
          defaultGenerator(physicalPlan.slotConfigurations(id), readOnly, codeGenerationMode)
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

  def parametersOrFail(state: QueryState): Array[AnyValue] = state match {
    case s: SlottedQueryState => s.params
    case _ => throw new InternalException(s"Expected a slotted query state")
  }
}

case class CompileWrappingDistinctGroupingExpression(grouping: CompiledGroupingExpression, isEmpty: Boolean) extends GroupingExpression {

  override def registerOwningPipe(pipe: Pipe): Unit = {}

  override type KeyType = AnyValue

  override def computeGroupingKey(context: ExecutionContext, state: QueryState): AnyValue =
    grouping.computeGroupingKey(context, state.query, CompiledExpressionConverter.parametersOrFail(state), state.cursors, state.expressionVariables)

  override def computeOrderedGroupingKey(groupingKey: AnyValue): AnyValue =
    throw new IllegalStateException("Compiled expressions do not support this yet.")

  override def getGroupingKey(context: ExecutionContext): AnyValue = grouping.getGroupingKey(context)

  override def project(context: ExecutionContext, groupingKey: AnyValue): Unit =
    grouping.projectGroupingKey(context, groupingKey)
}

case class CompileWrappingProjection(projection: CompiledProjection, isEmpty: Boolean) extends CommandProjection {

  override def registerOwningPipe(pipe: Pipe): Unit = {}

  override def project(ctx: ExecutionContext, state: QueryState): Unit =
    projection.project(ctx, state.query, CompiledExpressionConverter.parametersOrFail(state), state.cursors, state.expressionVariables)
}

case class CompileWrappingExpression(ce: CompiledExpression, legacy: Expression) extends ExtendedExpression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq(legacy)

  override def children: Seq[AstNode[_]] = Seq(legacy)

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    ce.evaluate(ctx, state.query, CompiledExpressionConverter.parametersOrFail(state), state.cursors, state.expressionVariables)

  override def toString: String = legacy.toString

  override val isDeterministic: Boolean = !legacy.exists {
    case RandFunction() => true
    case _              => false
  }
}

