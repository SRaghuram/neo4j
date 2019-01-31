/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.CodeGeneration.compileGroupingExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CodeGeneration, CompiledExpression, CompiledProjection, IntermediateCodeGeneration, _}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ExtendedExpression, RandFunction}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter.COMPILE_LIMIT
import org.neo4j.cypher.internal.v4_0.expressions.FunctionInvocation
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
import org.neo4j.helpers.Assertion.assertionsEnabled
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue

class CompiledExpressionConverter(log: Log, physicalPlan: PhysicalPlan, tokenContext: TokenContext, neverFail: Boolean = false) extends ExpressionConverter {

  //uses an inner converter to simplify compliance with Expression trait
  private val inner = new ExpressionConverters(SlottedExpressionConverters(physicalPlan), CommunityExpressionConverter(tokenContext))

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    //we don't deal with aggregations
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None

    case e => try {
      val ir = new IntermediateCodeGeneration(physicalPlan.slotConfigurations(id)).compileExpression(e)
      if (ir.nonEmpty) {
        log.debug(s"Compiling expression: $e")
      }
      ir.map(i => CompileWrappingExpression(CodeGeneration.compileExpression(i),
                                                         inner.toCommandExpression(id, expression)))
    } catch {
      case t: Throwable =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile expression: $e", t)
        None
    }
  }

  import org.neo4j.cypher.internal.v4_0.util.Foldable._

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    try {

      val slots = physicalPlan.slotConfigurations(id)
      val compiler = new IntermediateCodeGeneration(slots)
      val compiled = for {(k, v) <- projections
                          c <- compiler.compileExpression(v)} yield slots.get(k).get.offset -> c
      if (compiled.size < projections.size) None
      else {
        log.debug(s" Compiling projection: $projections")
        Some(CompileWrappingProjection(CodeGeneration.compileProjection(compiler.compileProjection(compiled)),
                                       projections.isEmpty))
      }
    }
    catch {
      case t: Throwable =>
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
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    try {
      val slots = physicalPlan.slotConfigurations(id)
      val compiler = new IntermediateCodeGeneration(slots)
      val compiled = for {(k, v) <- projections
                          c <- compiler.compileExpression(v)} yield slots(k) -> c
      if (compiled.size < projections.size) None
      else {
        log.debug(s" Compiling grouping expressions: $projections")
        Some(CompileWrappingDistinctGroupingExpression(
          compileGroupingExpression(compiler.compileGroupingExpression(compiled)), projections.isEmpty))
      }
    }
    catch {
      case t: Throwable =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        if (shouldThrow) throw t
        else log.debug(s"Failed to compile projection: $projections", t)
        None
    }
  }

  private def shouldThrow = !neverFail && assertionsEnabled()
}

case class CompileWrappingDistinctGroupingExpression(projection: CompiledGroupingExpression, isEmpty: Boolean) extends GroupingExpression {

  override def registerOwningPipe(pipe: Pipe): Unit = {}

  override type KeyType = AnyValue

  override def computeGroupingKey(context: ExecutionContext,
                                  state: QueryState): AnyValue =
    projection.computeGroupingKey(context, state.query, state.params, state.cursors)


  override def getGroupingKey(context: ExecutionContext): AnyValue = projection.getGroupingKey(context)

  override def project(context: ExecutionContext, groupingKey: AnyValue): Unit =
    projection.projectGroupingKey(context, groupingKey)
}

case class CompileWrappingProjection(projection: CompiledProjection, isEmpty: Boolean) extends CommandProjection {

  override def registerOwningPipe(pipe: Pipe): Unit = {}

  override def project(ctx: ExecutionContext, state: QueryState): Unit =
    projection.project(ctx, state.query, state.params, state.cursors)
}

case class CompileWrappingExpression(ce: CompiledExpression, legacy: Expression) extends ExtendedExpression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = legacy.arguments

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    ce.evaluate(ctx, state.query, state.params, state.cursors)

  override def symbolTableDependencies: Set[String] = legacy.symbolTableDependencies

  override def toString: String = legacy.toString

  override val isDeterministic: Boolean = !legacy.exists {
    case RandFunction() => true
    case _              => false
  }
}
