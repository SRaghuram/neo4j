/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.Plans.Pos
import com.neo4j.bench.micro.data.Plans.astVariable
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherType
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherValue
import org.neo4j.cypher.internal.spi.procsHelpers.asOption
import org.neo4j.internal.kernel
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI

import scala.collection.JavaConverters.asScalaBufferConverter

abstract class AbstractProcedureCall extends AbstractCypherBenchmark {
  protected var procedureSignature: ProcedureSignature = _

  /**
   * Called after starting of database
   */
  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    super.afterDatabaseStart(config)
    val resolver = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver
    val procs = resolver.resolveDependency(classOf[GlobalProcedures])

    val procName = procedureName(procs)
    val procedure = procs.procedure(new kernel.api.procs.QualifiedName(procName.namespace.toArray, procName.name))
    val signature = procedure.signature()
    val input = signature.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
      .toIndexedSeq
    val output = if (signature.isVoid) None else Some(
      signature.outputSignature().asScala
        .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated)).toIndexedSeq)

    procedureSignature = ProcedureSignature(procName,
      input,
      output,
      None,
      ProcedureReadOnlyAccess(signature.allowed()),
      asOption(signature.description()),
      asOption(signature.warning),
      procedure.signature().eager(),
      procedure.id())

  }

  def columns: Seq[String] = procedureSignature.outputSignature.map(f => f.map(_.name)).getOrElse(List.empty)

  def resolvedCall(callArguments: Seq[Expression]): ResolvedCall = {
    val callResults = columns.map(n => ProcedureResultItem(astVariable(n))(Pos)).toIndexedSeq
    ResolvedCall(procedureSignature, callArguments, callResults)(Pos)
  }

  def semanticTable: SemanticTable = {
    val semanticTypes = procedureSignature.outputSignature.getOrElse(IndexedSeq.empty).foldLeft(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]) {
      case (acc, current) => acc.updated(astVariable(current.name), ExpressionTypeInfo(current.typ, None))
    }
    SemanticTable(types = semanticTypes)
  }

  protected def procedureName(procedures: GlobalProcedures): QualifiedName
}