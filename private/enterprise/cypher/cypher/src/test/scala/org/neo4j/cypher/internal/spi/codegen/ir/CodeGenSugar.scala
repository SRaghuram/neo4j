/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen.ir

import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.internal.planner.v4_0.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.codegen._
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.runtime.compiled.{CompiledExecutionResult, CompiledPlan}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.TaskCloser
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.{QueryProfile, QueryResult, RuntimeResult}
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.time.Clocks
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.scalatest.mock.MockitoSugar

trait CodeGenSugar extends MockitoSugar with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {

  private val semanticTable = mock[SemanticTable]

  def compile(plan: LogicalPlan): CompiledPlan = {
    val statistics: GraphStatistics = mock[GraphStatistics]
    val context = mock[PlanContext]
    doReturn(statistics, Nil: _*).when(context).statistics
    new CodeGenerator(GeneratedQueryStructure, Clocks.systemClock())
      .generate(plan, context, semanticTable, readOnly = true, new StubCardinalities)
  }

  def compileAndProfile(plan: LogicalPlan, graphDb: GraphDatabaseQueryService): RuntimeResult = {
    val tx = graphDb.beginTransaction(Type.explicit, AnonymousContext.read())
    var transactionalContext: TransactionalContextWrapper = null
    try {
      val contextFactory = Neo4jTransactionalContextFactory.create(graphDb)
      transactionalContext = TransactionalContextWrapper(
        contextFactory.newContext( tx, "no query text exists for this test", EMPTY_MAP))
      val queryContext = new TransactionBoundQueryContext(transactionalContext)(mock[IndexSearchMonitor])
      val tracer = Some(new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider))
      val result = compile(plan).executionResultBuilder(queryContext, ProfileMode, tracer, EMPTY_MAP, false)
      result.accept(new QueryResultVisitor[Exception] {
        override def visit(row: QueryResult.Record): Boolean = true
      })
      tx.success()
      result
    } finally {
      transactionalContext.close(true)
      tx.close()
    }
  }

  def evaluate(instructions: Seq[Instruction],
               qtx: QueryContext = mockQueryContext(),
               columns: Seq[String] = Seq.empty,
               params: MapValue = EMPTY_MAP,
               operatorIds: Map[String, Id] = Map.empty): Seq[Map[String, Object]] = {
    val clazz = compile(instructions, columns, operatorIds)
    newInstance(clazz, queryContext = qtx, params = params).toList
  }

  def codeGenConfiguration = CodeGenConfiguration(mode = ByteCodeMode)

  def compile(instructions: Seq[Instruction], columns: Seq[String],
              operatorIds: Map[String, Id] = Map.empty): GeneratedQuery = {
    //In reality the same namer should be used for construction Instruction as in generating code
    //these tests separate the concerns so we give this namer non-standard prefixes
    CodeGenerator.generateCode(GeneratedQueryStructure)(instructions, operatorIds, columns, codeGenConfiguration)(
      new CodeGenContext(new SemanticTable(), columns.indices.map(i => columns(i) -> i).toMap, new Namer(
        new AtomicInteger(0), varPrefix = "TEST_VAR", methodPrefix = "TEST_METHOD"))).query
  }

  def newInstance(clazz: GeneratedQuery,
                  taskCloser: TaskCloser = new TaskCloser,
                  queryContext: QueryContext = mockQueryContext(),
                  tracer: Option[ProfilingTracer] = None,
                  params: MapValue = EMPTY_MAP): RewindableExecutionResult = {

    val generated = clazz.execute(queryContext,
                                  tracer.getOrElse(QueryExecutionTracer.NONE),
                                  params)

    val runtimeResult = new CompiledExecutionResult(queryContext, generated, tracer.getOrElse(QueryProfile.NONE), false)
    RewindableExecutionResult(runtimeResult, queryContext)
  }

  def insertStatic(clazz: Class[GeneratedQueryExecution], mappings: (String, Id)*) = mappings.foreach {
    case (name, id) => setStaticField(clazz, name, id.asInstanceOf[AnyRef])
  }

  private def mockQueryContext() = {
    val qc = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    when(qc.transactionalContext).thenReturn(transactionalContext)

    qc
  }
}
