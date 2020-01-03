/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.Plans.{Pos, astVariable}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.spi.procsHelpers.{asCypherType, asCypherValue, asOption}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.ast.{ASTAnnotationMap, ProcedureResultItem}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.internal.kernel.api.procs.{QualifiedName => KernelQualifiedName}
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI

import scala.collection.JavaConverters._

abstract class AbstractProcedureCall extends AbstractCypherBenchmark {
  protected var procedureSignature: ProcedureSignature = _

  /**
    * Called after starting of database
    */
  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    super.afterDatabaseStart(config)
    val resolver = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver
    val procs = resolver.resolveDependency(classOf[GlobalProcedures])

    val procedure = procs.procedure(new KernelQualifiedName(procedureName.namespace.toArray, procedureName.name))
    val signature = procedure.signature()
    val input = signature.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
      .toIndexedSeq
    val output = if (signature.isVoid) None else Some(
      signature.outputSignature().asScala
        .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated)).toIndexedSeq)

    procedureSignature = ProcedureSignature(procedureName,
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

  def resolvedCall: ResolvedCall = {
    val callResults = columns.map(n => ProcedureResultItem(astVariable(n))(Pos)).toIndexedSeq
    ResolvedCall(procedureSignature, Seq.empty, callResults)(Pos)
  }

  def semanticTable: SemanticTable = {
    val semanticTypes = procedureSignature.outputSignature.getOrElse(IndexedSeq.empty).foldLeft(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]) {
      case (acc, current) => acc.updated(astVariable(current.name), ExpressionTypeInfo(current.typ, None))
    }
    SemanticTable(types = semanticTypes)
  }

   protected def procedureName: QualifiedName
}
