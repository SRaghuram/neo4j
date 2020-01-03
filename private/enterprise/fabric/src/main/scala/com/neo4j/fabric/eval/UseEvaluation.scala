/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.util.function.Supplier

import com.neo4j.fabric.util.Errors
import com.neo4j.fabric.util.Rewritten._
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.ast.{CatalogName, UseGraph}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.internal.kernel.api.{QueryContext => _}
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._
import scala.collection.mutable

case class UseEvaluation(
  catalog: Catalog,
  proceduresSupplier: Supplier[GlobalProcedures],
  signatureResolver: ProcedureSignatureResolver,
) {

  private val evaluator = new StaticEvaluation.StaticEvaluator(proceduresSupplier)

  def evaluate(
    originalStatement: String,
    use: UseGraph,
    parameters: MapValue,
    context: java.util.Map[String, AnyValue]
  ): Catalog.Graph =
    evaluate(originalStatement, use, parameters, context.asScala)

  def evaluate(
    originalStatement: String,
    use: UseGraph,
    parameters: MapValue,
    context: mutable.Map[String, AnyValue]
  ): Catalog.Graph = Errors.errorContext(originalStatement, use) {

    use.expression match {
      case v: Variable =>
        catalog.resolve(nameFromVar(v))

      case p: Property =>
        catalog.resolve(nameFromProp(p))

      case f: FunctionInvocation =>
        val ctx = ExecutionContext(context)
        val argValues = f.args
          .map(resolveFunctions)
          .map(expr => evaluator.evaluate(expr, parameters, ctx))
        catalog.resolve(nameFromFunc(f), argValues)

      case x =>
        Errors.openCypherUnexpected("graph or view reference", x)
    }
  }

  def resolveFunctions(expr: Expression): Expression = expr.rewritten.bottomUp {
    case f: FunctionInvocation if f.needsToBeResolved => {
      val resolved = ResolvedFunctionInvocation(signatureResolver.functionSignature)(f).coerceArguments

      if (resolved.fcnSignature.isEmpty) {
        Errors.openCypherFailure(Errors.openCypherSemantic(s"Unknown function '${resolved.qualifiedName}'", resolved))
      }
      
      return resolved
    }
  }

  private def nameFromVar(variable: Variable): CatalogName =
    CatalogName(variable.name)

  private def nameFromProp(property: Property): CatalogName = {
    def parts(expr: Expression): List[String] = expr match {
      case p: Property    => parts(p.map) :+ p.propertyKey.name
      case Variable(name) => List(name)
      case x              => Errors.openCypherUnexpected("Graph name segment", x)
    }

    CatalogName(parts(property))
  }

  private def nameFromFunc(func: FunctionInvocation): CatalogName = {
    CatalogName(func.namespace.parts :+ func.functionName.name)
  }
}
