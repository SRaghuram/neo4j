/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.Assertion.assertionsEnabled
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.CodeGeneration.compileGroupingExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.CodeGeneration.compileClass
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateCodeGeneration.defaultGenerator
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CodeGeneration, CompiledExpression, CompiledProjection, _}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ExtendedExpression, RandFunction}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.v4_0.expressions.FunctionInvocation
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
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
      val ir = defaultGenerator(physicalPlan.slotConfigurations(id)).compileExpression(e)
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

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    try {

      val slots = physicalPlan.slotConfigurations(id)
      val compiler = defaultGenerator(slots)
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
                                    orderToLeverage: Seq[ast.Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    try {
      if(orderToLeverage.nonEmpty) {
        // TODO Support compiled ordered GroupingExpression
        None
      } else {
        val slots = physicalPlan.slotConfigurations(id)
        val compiler = defaultGenerator(slots)
        val compiled = for {(k, v) <- projections
                            c <- compiler.compileExpression(v)} yield slots(k) -> c
        if (compiled.size < projections.size) None
        else {
          log.debug(s" Compiling grouping expressions: $projections")
          val declaration = compileGroupingClassDeclaration(compiler.compileGroupingExpression(compiled))
          Some(CompileWrappingDistinctGroupingExpression(
            compileClass(declaration).getDeclaredConstructor().newInstance().asInstanceOf[CompiledGroupingExpression],
            projections.isEmpty))
        }
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

object CompiledExpressionConverter {
  def parametersOrFail(state: QueryState): Array[AnyValue] = state match {
    case s: SlottedQueryState => s.params
    case _ => throw new InternalException(s"Expected a slotted query state")
  }

  def compileExpressionClassDeclaration(expression: IntermediateExpression): ClassDeclaration = {
      val declarations = block(expression.variables.distinct.map { v =>
        declareAndAssign(v.typ, v.name, v.value)
      }: _*)

      ClassDeclaration(
        PACKAGE_NAME,
        className(),
        None,
        Seq(typeRefOf[CompiledExpression]),
        Seq.empty,
        initializationCode = noop(),
        fields = expression.fields,
        methods = Seq(
          MethodDeclaration("evaluate",
                            owner = typeRefOf[CompiledExpression],
                            returnType = typeRefOf[AnyValue],
                            parameters = Seq(param[ExecutionContext]("context"),
                                             param[DbAccess]("dbAccess"),
                                             param[Array[AnyValue]]("params"),
                                             param[ExpressionCursors]("cursors"),
                                             param[Array[AnyValue]]("expressionVariables")),
                            body = block(
                              declarations,
                              returns(nullCheck(expression)(expression.ir))
                            ))))
  }

  def compileProjectionClassDeclaration(expression: IntermediateExpression): ClassDeclaration = {
    val declarations = block(expression.variables.distinct.map { v =>
      declareAndAssign(v.typ, v.name, v.value)
    }: _*)

    ClassDeclaration(
      PACKAGE_NAME,
      className(),
      None,
      Seq(typeRefOf[CompiledProjection]),
      Seq.empty,
      initializationCode = noop(),
      fields = expression.fields,
      methods = Seq(
        MethodDeclaration("project",
                          owner = typeRefOf[CompiledProjection],
                          returnType = typeRefOf[Unit],
                          parameters = Seq(param[ExecutionContext]("context"),
                                           param[DbAccess]("dbAccess"),
                                           param[Array[AnyValue]]("params"),
                                           param[ExpressionCursors]("cursors"),
                                           param[Array[AnyValue]]("expressionVariables")),
                          body = block(
                            declarations,
                            expression.ir
                          ))))
  }

  def compileGroupingClassDeclaration(grouping: IntermediateGroupingExpression): ClassDeclaration = {
    def declarations(e: IntermediateExpression) = block(e.variables.distinct.map { v =>
      declareAndAssign(v.typ, v.name, v.value)
    }: _*)

    ClassDeclaration(
      PACKAGE_NAME,
      className(),
      None,
      Seq(typeRefOf[CompiledGroupingExpression]),
      Seq.empty,
      initializationCode = noop(),
      fields = grouping.projectKey.fields ++ grouping.computeKey.fields ++ grouping.getKey.fields,
      methods = Seq(
        MethodDeclaration("projectGroupingKey",
                          owner = typeRefOf[CompiledGroupingExpression],
                          returnType = typeRefOf[Unit],
                          parameters = Seq(param[ExecutionContext]("context"),
                                           param[AnyValue]("key")),
                          body = block(
                            declarations(grouping.projectKey),
                            grouping.projectKey.ir)),
        MethodDeclaration("computeGroupingKey",
                          owner = typeRefOf[CompiledGroupingExpression],
                          returnType = typeRefOf[AnyValue],
                          parameters = Seq(param[ExecutionContext]("context"),
                            param[DbAccess]("dbAccess"),
                            param[Array[AnyValue]]("params"),
                            param[ExpressionCursors]("cursors"),
                            param[Array[AnyValue]]("expressionVariables")),
                          body = block(
                              declarations(grouping.computeKey),
                              returns(nullCheck(grouping.computeKey)(grouping.computeKey.ir)))),
        MethodDeclaration("getGroupingKey",
                          owner = typeRefOf[CompiledGroupingExpression],
                          returnType = typeRefOf[AnyValue],
                          parameters = Seq(param[ExecutionContext]("context")),
                          body = block(
                            declarations(grouping.getKey),
                            returns(nullCheck(grouping.getKey)(grouping.getKey.ir))))
        ))
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

  override def arguments: Seq[Expression] = legacy.arguments

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    ce.evaluate(ctx, state.query, CompiledExpressionConverter.parametersOrFail(state), state.cursors, state.expressionVariables)

  override def symbolTableDependencies: Set[String] = legacy.symbolTableDependencies

  override def toString: String = legacy.toString

  override val isDeterministic: Boolean = !legacy.exists {
    case RandFunction() => true
    case _              => false
  }
}

