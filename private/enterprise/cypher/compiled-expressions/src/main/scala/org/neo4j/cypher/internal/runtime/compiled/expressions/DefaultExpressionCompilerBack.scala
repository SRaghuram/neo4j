/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.ClassDeclaration
import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode
import org.neo4j.codegen.api.CodeGeneration.compileAnonymousClass
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.param
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.MethodDeclaration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.DefaultExpressionCompilerBack.PACKAGE_NAME
import org.neo4j.cypher.internal.runtime.compiled.expressions.DefaultExpressionCompilerBack.className
import org.neo4j.values.AnyValue

/**
  * Compiles [[IntermediateExpression]] into compiled byte code.
  */
class DefaultExpressionCompilerBack(codeGenerationMode: CodeGenerationMode) extends ExpressionCompilerBack {

  def compileExpression(expression: IntermediateExpression): CompiledExpression = {
    val classDeclaration =
      ClassDeclaration[CompiledExpression](
        PACKAGE_NAME,
        className(),
        None,
        Seq(typeRefOf[CompiledExpression]),
        Seq.empty,
        initializationCode = noop(),
        genFields = () => expression.fields,
        methods = Seq(
          MethodDeclaration("evaluate",
            returnType = typeRefOf[AnyValue],
            parameters = Seq(param[ReadableRow](ExpressionCompilation.ROW_NAME),
              param[DbAccess](ExpressionCompilation.DB_ACCESS_NAME),
              param[Array[AnyValue]](ExpressionCompilation.PARAMS_NAME),
              param[ExpressionCursors](ExpressionCompilation.CURSORS_NAME),
              param[Array[AnyValue]](ExpressionCompilation.EXPRESSION_VARIABLES_NAME)),
            body = block(
              block(expression.variables.distinct.map { v =>
                declareAndAssign(v.typ, v.name, v.value)
              }: _*),
              nullCheckIfRequired(expression)
            ))))
    compileAnonymousClass(classDeclaration, CodeGeneration.createGenerator(codeGenerationMode)).getDeclaredConstructor().newInstance()
  }

  def compileProjection(projection: IntermediateExpression): CompiledProjection = {
    val classDeclaration =
      ClassDeclaration[CompiledProjection](
        PACKAGE_NAME,
        className(),
        None,
        Seq(typeRefOf[CompiledProjection]),
        Seq.empty,
        initializationCode = noop(),
        genFields = () => projection.fields,
        methods = Seq(
          MethodDeclaration("project",
            returnType = typeRefOf[Unit],
            parameters = Seq(param[ReadWriteRow](ExpressionCompilation.ROW_NAME),
              param[DbAccess](ExpressionCompilation.DB_ACCESS_NAME),
              param[Array[AnyValue]](ExpressionCompilation.PARAMS_NAME),
              param[ExpressionCursors](ExpressionCompilation.CURSORS_NAME),
              param[Array[AnyValue]](ExpressionCompilation.EXPRESSION_VARIABLES_NAME)),
            body = block(
              block(projection.variables.distinct.map { v =>
                declareAndAssign(v.typ, v.name, v.value)
              }: _*),
              projection.ir
            ))))
      compileAnonymousClass(classDeclaration, CodeGeneration.createGenerator(codeGenerationMode)).getDeclaredConstructor().newInstance()
  }

  def compileGrouping(grouping: IntermediateGroupingExpression): CompiledGroupingExpression = {
    def declarations(e: IntermediateExpression): IntermediateRepresentation =
      block(e.variables.distinct.map { v =>
        declareAndAssign(v.typ, v.name, v.value)
      }: _*)

    val classDeclaration =
      ClassDeclaration[CompiledGroupingExpression](
        PACKAGE_NAME,
        className(),
        None,
        Seq(typeRefOf[CompiledGroupingExpression]),
        Seq.empty,
        initializationCode = noop(),
        genFields = () => grouping.projectKey.fields ++ grouping.computeKey.fields ++ grouping.getKey.fields,
        methods = Seq(
          MethodDeclaration("projectGroupingKey",
            returnType = typeRefOf[Unit],
            parameters = Seq(param[WritableRow](ExpressionCompilation.ROW_NAME),
              param[AnyValue](ExpressionCompilation.GROUPING_KEY_NAME)),
            body = block(
              declarations(grouping.projectKey),
              grouping.projectKey.ir)),
          MethodDeclaration("computeGroupingKey",
            returnType = typeRefOf[AnyValue],
            parameters = Seq(param[ReadableRow](ExpressionCompilation.ROW_NAME),
              param[DbAccess](ExpressionCompilation.DB_ACCESS_NAME),
              param[Array[AnyValue]](ExpressionCompilation.PARAMS_NAME),
              param[ExpressionCursors](ExpressionCompilation.CURSORS_NAME),
              param[Array[AnyValue]](ExpressionCompilation.EXPRESSION_VARIABLES_NAME)),
            body = block(
              declarations(grouping.computeKey),
              nullCheckIfRequired(grouping.computeKey))),
          MethodDeclaration("getGroupingKey",
            returnType = typeRefOf[AnyValue],
            parameters = Seq(param[CypherRow](ExpressionCompilation.ROW_NAME)),
            body = block(
              declarations(grouping.getKey),
              nullCheckIfRequired(grouping.getKey)))
        ))
    compileAnonymousClass(classDeclaration, CodeGeneration.createGenerator(codeGenerationMode)).getDeclaredConstructor().newInstance()
  }
}

object DefaultExpressionCompilerBack {
  private val COUNTER = new AtomicLong(0L)
  private val PACKAGE_NAME = "org.neo4j.codegen"
  private def className(): String = "Expression" + COUNTER.getAndIncrement()
}
