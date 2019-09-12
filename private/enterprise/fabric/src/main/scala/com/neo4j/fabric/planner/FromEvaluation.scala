/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner

import java.util.Optional

import com.neo4j.fabric.utils.Rewritten._
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.ast.{CatalogName, FromGraph}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{DefaultParameterValue, Neo4jTypes}
import org.neo4j.internal.kernel.api.{QueryContext => _, _}
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._
import scala.collection.mutable

case class FromEvaluation(catalog: Catalog, proceduresSupplier: ()=>GlobalProcedures ) {

  private val evaluator = new StaticEvaluation.StaticEvaluator(proceduresSupplier)

  def evaluate(
    from: FromGraph,
    parameters: MapValue,
    context: java.util.Map[String, AnyValue]
  ): Catalog.Graph =
    evaluate(from, parameters, context.asScala)

  def evaluate(
    from: FromGraph,
    parameters: MapValue,
    context: mutable.Map[String, AnyValue]
  ): Catalog.Graph = Errors.errorContext(from) {

    from.expression match {
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
        Errors.unexpected("graph or view reference", x)
    }
  }

  def resolveFunctions(expr: Expression): Expression = expr.rewritten.bottomUp {
    case f: FunctionInvocation if f.needsToBeResolved =>
      ResolvedFunctionInvocation(functionSignature)(f).coerceArguments
  }

  private def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val fcn = Option(proceduresSupplier.apply().function(asKernelQualifiedName(name)))
      .getOrElse(Errors.notFound("Function", name.toString, InputPosition.NONE))

    val signature = fcn.signature()

    Some(UserFunctionSignature(
      name = name,
      inputSignature = signature.inputSignature().asScala.toIndexedSeq.map(s => FieldSignature(
        name = s.name(),
        typ = asCypherType(s.neo4jType()),
        default = s.defaultValue().asScala.map(asCypherValue))),
      outputType = asCypherType(signature.outputType()),
      deprecationInfo = signature.deprecated().asScala,
      allowed = signature.allowed(),
      description = signature.description().asScala,
      isAggregate = false,
      id = fcn.id(),
      threadSafe = fcn.threadSafe()
    ))
  }


  private def asKernelQualifiedName(name: QualifiedName): procs.QualifiedName =
    new procs.QualifiedName(name.namespace.toArray, name.name)

  private def asCypherValue(neo4jValue: DefaultParameterValue) =
    CypherValue(neo4jValue.value, asCypherType(neo4jValue.neo4jType()))

  private def asCypherType(neoType: AnyType): CypherType = neoType match {
    case Neo4jTypes.NTString        => CTString
    case Neo4jTypes.NTInteger       => CTInteger
    case Neo4jTypes.NTFloat         => CTFloat
    case Neo4jTypes.NTNumber        => CTNumber
    case Neo4jTypes.NTBoolean       => CTBoolean
    case l: Neo4jTypes.ListType     => CTList(asCypherType(l.innerType()))
    case Neo4jTypes.NTByteArray     => CTList(CTAny)
    case Neo4jTypes.NTDateTime      => CTDateTime
    case Neo4jTypes.NTLocalDateTime => CTLocalDateTime
    case Neo4jTypes.NTDate          => CTDate
    case Neo4jTypes.NTTime          => CTTime
    case Neo4jTypes.NTLocalTime     => CTLocalTime
    case Neo4jTypes.NTDuration      => CTDuration
    case Neo4jTypes.NTPoint         => CTPoint
    case Neo4jTypes.NTNode          => CTNode
    case Neo4jTypes.NTRelationship  => CTRelationship
    case Neo4jTypes.NTPath          => CTPath
    case Neo4jTypes.NTGeometry      => CTGeometry
    case Neo4jTypes.NTMap           => CTMap
    case Neo4jTypes.NTAny           => CTAny
  }

  private implicit class OptionalOps[T](optional: Optional[T]) {
    def asScala: Option[T] =
      if (optional.isPresent) Some(optional.get()) else None
  }


  private def nameFromVar(variable: Variable): CatalogName =
    CatalogName(variable.name)

  private def nameFromProp(property: Property): CatalogName = {
    def parts(expr: Expression): List[String] = expr match {
      case p: Property    => parts(p.map) :+ p.propertyKey.name
      case Variable(name) => List(name)
      case x              => Errors.unexpected("Graph name segment", x)
    }

    CatalogName(parts(property))
  }

  private def nameFromFunc(func: FunctionInvocation): CatalogName = {
    CatalogName(func.namespace.parts :+ func.functionName.name)
  }
}
