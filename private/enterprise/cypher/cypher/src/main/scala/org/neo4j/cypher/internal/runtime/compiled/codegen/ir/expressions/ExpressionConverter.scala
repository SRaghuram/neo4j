/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.functions.functionConverter
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.cypher.internal

object ExpressionConverter {

  implicit class ExpressionToPredicate(expression: CodeGenExpression) {

    def asPredicate = new CodeGenExpression {

      override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
        if (expression.nullable || !expression.codeGenType.isPrimitive) structure.coerceToBoolean(expression.generateExpression(structure))
        else expression.generateExpression(structure)
      }

      override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = expression.init(generator)

      override def nullable(implicit context: CodeGenContext) = false

      override def codeGenType(implicit context: CodeGenContext) =
        if (nullable) CypherCodeGenType(CTBoolean, ReferenceType)
        else expression.codeGenType
    }
  }

  def createPredicate(expression: internal.expressions.Expression)
                     (implicit context: CodeGenContext): CodeGenExpression = expression match {
    case internal.expressions.HasLabels(x:internal.expressions.LogicalVariable, label :: Nil) =>
      val labelIdVariable = context.namer.newVarName()
      val nodeVariable = context.getVariable(x.name)
      HasLabel(nodeVariable, labelIdVariable, label.name).asPredicate

    case exp@internal.expressions.Property(x:internal.expressions.LogicalVariable, propKey) if context.semanticTable.isNode(x) =>
      createExpression(exp).asPredicate

    case exp@internal.expressions.Property(x:internal.expressions.LogicalVariable, propKey) if context.semanticTable.isRelationship(x) =>
      createExpression(exp).asPredicate

    case internal.expressions.Not(e) => Not(createExpression(e)).asPredicate

    case internal.expressions.Equals(lhs, rhs) => Equals(createExpression(lhs), createExpression(rhs)).asPredicate

    case internal.expressions.Or(lhs, rhs) => Or(createExpression(lhs), createExpression(rhs)).asPredicate

    case exp: internal.expressions.LogicalVariable =>
      createExpression(exp).asPredicate

    case _:internal.expressions.False => False
    case _:internal.expressions.True => True

    case CoerceToPredicate(inner) => createPredicate(inner)

    case other =>
      throw new CantCompileQueryException(s"Predicate of $other not yet supported")

  }

  def createExpression(expression: internal.expressions.Expression)
                      (implicit context: CodeGenContext): CodeGenExpression = expressionConverter(expression, createExpression)

  def createMaterializeExpressionForVariable(variableQueryVariable: String)
                                            (implicit context: CodeGenContext): CodeGenExpression = {

    val variable = context.getVariable(variableQueryVariable)

    variable.codeGenType match {
      case CypherCodeGenType(CTNode, _) => NodeProjection(variable)
      case CypherCodeGenType(CTRelationship, _) => RelationshipProjection(variable)
      case CypherCodeGenType(CTString, _) |
           CypherCodeGenType(CTBoolean, _) |
           CypherCodeGenType(CTInteger, _) |
           CypherCodeGenType(CTFloat, _) =>
        LoadVariable(variable)
      case CypherCodeGenType(ListType(CTInteger), ListReferenceType(LongType)) =>
        // TODO: PrimitiveProjection(variable)
        AnyProjection(variable) // Temporarily resort to runtime projection
      case CypherCodeGenType(ListType(CTFloat), ListReferenceType(FloatType)) =>
        // TODO: PrimitiveProjection(variable)
        AnyProjection(variable) // Temporarily resort to runtime projection
      case CypherCodeGenType(ListType(CTBoolean), ListReferenceType(BoolType)) =>
        // TODO: PrimitiveProjection(variable)
        AnyProjection(variable) // Temporarily resort to runtime projection
      case CypherCodeGenType(ListType(CTString), _) |
           CypherCodeGenType(ListType(CTBoolean), _) |
           CypherCodeGenType(ListType(CTInteger), _) |
           CypherCodeGenType(ListType(CTFloat), _) =>
        LoadVariable(variable)
      case CypherCodeGenType(CTAny, _) => AnyProjection(variable)
      case CypherCodeGenType(CTMap, _) => AnyProjection(variable)
      case CypherCodeGenType(ListType(_), _) => AnyProjection(variable) // TODO: We could have a more specialized projection when the inner type is known to be node or relationship
      case _ => throw new CantCompileQueryException(s"The compiled runtime cannot handle results of type ${variable.codeGenType}")
    }
  }

  private def expressionConverter(expression: internal.expressions.Expression, callback: internal.expressions.Expression => CodeGenExpression)
                                 (implicit context: CodeGenContext): CodeGenExpression = {

    expression match {
      case node:internal.expressions.LogicalVariable if context.semanticTable.isNode(node) =>
        NodeExpression(context.getVariable(node.name))

      case rel:internal.expressions.LogicalVariable if context.semanticTable.isRelationship(rel) =>
        RelationshipExpression(context.getVariable(rel.name))

      case internal.expressions.Property(node:internal.expressions.LogicalVariable, propKey) if context.semanticTable.isNode(node) =>
        val token = context.semanticTable.id(propKey).map(_.id)
        NodeProperty(token, propKey.name, context.getVariable(node.name), context.namer.newVarName())

      case internal.expressions.Property(rel:internal.expressions.LogicalVariable, propKey) if context.semanticTable.isRelationship(rel) =>
        val token = context.semanticTable.id(propKey).map(_.id)
        RelProperty(token, propKey.name, context.getVariable(rel.name), context.namer.newVarName())

      case internal.expressions.Property(mapExpression, internal.expressions.PropertyKeyName(propKeyName)) =>
        MapProperty(callback(mapExpression), propKeyName)

      case internal.expressions.Parameter(name, cypherType) =>
        // Parameters always comes as AnyValue
        expressions.Parameter(name, context.namer.newVarName(), CypherCodeGenType(cypherType, AnyValueType))

      case lit: internal.expressions.IntegerLiteral => Literal(lit.value)

      case lit: internal.expressions.DoubleLiteral => Literal(lit.value)

      case lit: internal.expressions.StringLiteral => Literal(lit.value)

      case lit: internal.expressions.Literal => Literal(lit.value)

      case internal.expressions.ListLiteral(exprs) =>
        expressions.ListLiteral(exprs.map(e => callback(e)))

      case internal.expressions.Add(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Addition(leftOp, rightOp)

      case internal.expressions.Subtract(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Subtraction(leftOp, rightOp)

      case internal.expressions.Multiply(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Multiplication(leftOp, rightOp)

      case internal.expressions.Divide(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Division(leftOp, rightOp)

      case internal.expressions.Modulo(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Modulo(leftOp, rightOp)

      case internal.expressions.Pow(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Pow(leftOp, rightOp)

      case internal.expressions.MapExpression(items) =>
        val map = items.map {
          case (key, expr) => (key.name, callback(expr))
        }.toMap
        MyMap(map)

      case internal.expressions.HasLabels(x:internal.expressions.LogicalVariable, label :: Nil) =>
        val labelIdVariable = context.namer.newVarName()
        val nodeVariable = context.getVariable(x.name)
        HasLabel(nodeVariable, labelIdVariable, label.name)

      case internal.expressions.Equals(lhs, rhs) => Equals(callback(lhs), callback(rhs))

      case internal.expressions.Or(lhs, rhs) => Or(callback(lhs), callback(rhs))

      case internal.expressions.Not(inner) => Not(callback(inner))

      case f: internal.expressions.FunctionInvocation => functionConverter(f, callback)

      case x: internal.expressions.LogicalVariable => LoadVariable(context.getVariable(x.name))

      case other => throw new CantCompileQueryException(s"Expression of $other not yet supported")
    }
  }

}
