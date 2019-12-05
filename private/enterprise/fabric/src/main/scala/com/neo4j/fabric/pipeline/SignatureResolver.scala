/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.pipeline

import java.util.Optional
import java.util.function.Supplier

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTList, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType}
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{DefaultParameterValue, Neo4jTypes}
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.procedure.Mode

import scala.collection.JavaConverters._

class SignatureResolver(
  registrySupplier: Supplier[GlobalProcedures]
) extends ProcedureSignatureResolver {

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    Option(registrySupplier.get().function(asKernelQualifiedName(name)))
      .map { fcn =>

        val signature = fcn.signature()

        UserFunctionSignature(
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
        )
      }
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val handle = registrySupplier.get().procedure(kn)
    val signature = handle.signature()

    ProcedureSignature(
      name = name,
      inputSignature = signature.inputSignature().asScala.toIndexedSeq.map(s => FieldSignature(
        name = s.name(),
        typ = asCypherType(s.neo4jType()),
        default = s.defaultValue().asScala.map(asCypherValue))),
      outputSignature = if (signature.isVoid)
        None else
        Some(signature.outputSignature().asScala.toIndexedSeq.map(s => FieldSignature(
          name = s.name(),
          typ = asCypherType(s.neo4jType()),
          deprecated = s.isDeprecated))),
      deprecationInfo = signature.deprecated().asScala,
      accessMode = asCypherProcMode(signature.mode(), signature.allowed()),
      description = signature.description().asScala,
      warning = signature.warning().asScala,
      eager = signature.eager(),
      id = handle.id(),
      systemProcedure = signature.systemProcedure()
    )
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

  def asCypherProcMode(mode: Mode, allowed: Array[String]): ProcedureAccessMode = mode match {
    case Mode.READ    => ProcedureReadOnlyAccess(allowed)
    case Mode.DEFAULT => ProcedureReadOnlyAccess(allowed)
    case Mode.WRITE   => ProcedureReadWriteAccess(allowed)
    case Mode.SCHEMA  => ProcedureSchemaWriteAccess(allowed)
    case Mode.DBMS    => ProcedureDbmsAccess(allowed)

    case _ => throw new CypherExecutionException(
      "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode.name(), null)
  }

  private implicit class OptionalOps[T](optional: Optional[T]) {
    def asScala: Option[T] =
      if (optional.isPresent) Some(optional.get()) else None
  }
}
