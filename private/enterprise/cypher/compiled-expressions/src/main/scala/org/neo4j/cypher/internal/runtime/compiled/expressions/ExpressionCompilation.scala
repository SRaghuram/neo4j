/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object ExpressionCompilation {
  val DB_ACCESS_NAME: String = "dbAccess"
  val DB_ACCESS: IntermediateRepresentation = load[DbAccess](DB_ACCESS_NAME)

  val PARAMS_NAME: String = "params"
  val PARAMS: IntermediateRepresentation = load[MapValue](PARAMS_NAME)

  val CURSORS_NAME = "cursors"
  val CURSORS: IntermediateRepresentation = load[ExpressionCursors](CURSORS_NAME)

  val vNODE_CURSOR: LocalVariable = cursorVariable[NodeCursor]("nodeCursor")
  val NODE_CURSOR: IntermediateRepresentation = load(vNODE_CURSOR)

  val vRELATIONSHIP_CURSOR: LocalVariable = cursorVariable[RelationshipScanCursor]("relationshipScanCursor")
  val RELATIONSHIP_CURSOR: IntermediateRepresentation = load(vRELATIONSHIP_CURSOR)

  val vPROPERTY_CURSOR: LocalVariable = cursorVariable[PropertyCursor]("propertyCursor")
  val PROPERTY_CURSOR: IntermediateRepresentation = load(vPROPERTY_CURSOR)

  private def cursorVariable[T](name: String)(implicit m: Manifest[T]): LocalVariable =
    variable[T](name, invoke(CURSORS, method[ExpressionCursors, T](name)))

  val vCURSORS = Seq(vNODE_CURSOR, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR)

  val ROW_NAME: String = "row"
  val ROW: IntermediateRepresentation = load[ReadableRow](ROW_NAME)

  val EXPRESSION_VARIABLES_NAME: String = "expressionVariables"
  val EXPRESSION_VARIABLES: IntermediateRepresentation = load[Array[AnyValue]](EXPRESSION_VARIABLES_NAME)

  def setExpressionVariable(ev: ExpressionVariable, value: IntermediateRepresentation): IntermediateRepresentation = {
    arraySet(ExpressionCompilation.EXPRESSION_VARIABLES, ev.offset, value)
  }

  def loadExpressionVariable(ev: ExpressionVariable): IntermediateRepresentation = {
    arrayLoad(ExpressionCompilation.EXPRESSION_VARIABLES, ev.offset)
  }

  val GROUPING_KEY_NAME: String = "key"

  def noValueOr(expressions: IntermediateExpression*)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    nullCheck(expressions:_*)(noValue)(onNotNull)
  }

  def nullCheck(expressions: IntermediateExpression*)(onNull: IntermediateRepresentation = noValue)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    val checks = expressions.foldLeft(Set.empty[IntermediateRepresentation])((acc, current) => acc ++ current.nullChecks)
    if (checks.nonEmpty) ternary(or(checks.toSeq), onNull, onNotNull)
    else onNotNull
  }

  def nullCheckIfRequired(expression: IntermediateExpression, onNull: IntermediateRepresentation = noValue): IntermediateRepresentation =
    if (expression.requireNullCheck) nullCheck(expression)(onNull)(expression.ir) else expression.ir
}
