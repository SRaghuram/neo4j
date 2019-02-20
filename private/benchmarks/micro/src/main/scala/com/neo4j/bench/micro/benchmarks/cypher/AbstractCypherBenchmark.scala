/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util
import java.util.function.LongSupplier

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark
import org.neo4j.cypher.internal.compatibility.v3_3.WrappedMonitors
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.{BuildCompiledExecutionPlan, EnterpriseRuntimeContext}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{BuildInterpretedExecutionPlan, NormalMode}
import org.neo4j.cypher.internal.compiler.v3_3.CostBasedPlannerName
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_3.phases.devNullLogger
import org.neo4j.cypher.internal.frontend.v3_3.{PlannerName, SemanticTable}
import org.neo4j.cypher.internal.spi.v3_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_3.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.spi.v3_3.{QueryContext, TransactionBoundPlanContext, TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.{BuildSlottedExecutionPlan, InternalExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.{KernelTransaction, Statement}
import org.neo4j.kernel.guard.Guard
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContext, TransactionalContext}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.resources.{CpuClock, HeapAllocation}
import org.neo4j.time.Clocks
import org.neo4j.values.virtual.{MapValue, VirtualValues}
import org.openjdk.jmh.infra.Blackhole

abstract class AbstractCypherBenchmark extends BaseDatabaseBenchmark {
  private val defaultPlannerName: PlannerName = CostBasedPlannerName.default

  class CountVisitor(bh: Blackhole) extends QueryResultVisitor[Exception] {
    var count: Int = 0

    override def visit(row: Record): Boolean = {
      count += 1
      bh.consume(row)
      true
    }
  }

  @Override
  def benchmarkGroup = "Cypher"

  @Override
  def isThreadSafe = false

  def getLogicalPlanAndSemanticTable(planContext: PlanContext): (LogicalPlan, SemanticTable, List[String])

  def assertExpectedRowCount(expectedRowCount: Int, visitor: CountVisitor): Int =
    if (visitor.count != expectedRowCount) {
      val actualCount = visitor.count
      throw new RuntimeException(s"Expected $expectedRowCount results but found $actualCount")
    } else visitor.count

  def assertExpectedRowCount(minRowCount: Int, maxRowCount: Int, visitor: CountVisitor): Int =
    if (minRowCount > visitor.count || visitor.count > maxRowCount) {
      val actualCount = visitor.count
      throw new RuntimeException(s"Expected result count in range ($minRowCount,$maxRowCount) but found $actualCount")
    } else visitor.count

  def beginInternalTransaction(): InternalTransaction =
    new GraphDatabaseCypherService(db).beginTransaction(KernelTransaction.Type.explicit, SecurityContext.AUTH_DISABLED)

  def buildPlan(cypherRuntime: CypherRuntime): InternalExecutionResultBuilder = {
    val tx: InternalTransaction = beginInternalTransaction()
    try {
      val planContext: PlanContext = getPlanContext(tx)
      val runtimeContext = getContext(cypherRuntime, planContext)
      val (logicalPlan, semanticTable, resultColumns) = getLogicalPlanAndSemanticTable(planContext)
      //since we do "manual planning" we need to call assignIds once we are done
      logicalPlan.assignIds()
      val compilationStateBefore = getCompilationState(logicalPlan, semanticTable, resultColumns)
      val compilationStateAfter: CompilationState = cypherRuntime match {
        case Interpreted => BuildInterpretedExecutionPlan.process(compilationStateBefore, runtimeContext)
        case EnterpriseInterpreted => BuildSlottedExecutionPlan.process(compilationStateBefore, runtimeContext)
        case CompiledByteCode => BuildCompiledExecutionPlan.process(compilationStateBefore, runtimeContext)
        case CompiledSourceCode => BuildCompiledExecutionPlan.process(compilationStateBefore, runtimeContext)
        case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntime")
      }
      val executionPlan: ExecutionPlan = compilationStateAfter.maybeExecutionPlan.getOrElse(
        throw new RuntimeException(s"Failed to build an execution plan with runtime: $cypherRuntime"))
      new InternalExecutionResultBuilder((params, tx) => {
        val queryContext = getQueryContext(tx)
        executionPlan.run(queryContext, NormalMode, params)
      })
    }
    finally {
      tx.close()
    }
  }

  private def getContext(cypherRuntime: CypherRuntime, planContext: PlanContext): EnterpriseRuntimeContext = ContextHelper.create(
    monitors = WrappedMonitors(kernelMonitors),
    codeStructure = GeneratedQueryStructure,
    planContext = planContext,
    debugOptions = cypherRuntime.debugOptions)

  private def getPlanContext(tx: InternalTransaction): PlanContext =
    new TransactionBoundPlanContext(transactionalContextWrapper(tx), devNullLogger)

  private def getQueryContext(tx: InternalTransaction): QueryContext = {
    val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
    new TransactionBoundQueryContext(transactionalContextWrapper(tx))(searchMonitor)
  }

  private def kernelMonitors = dependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])

  private def transactionalContextWrapper(tx: InternalTransaction) = TransactionalContextWrapper(txContext(tx))

  private def getCompilationState(logicalPlan: LogicalPlan, semanticTable: SemanticTable, resultColumns: List[String]): LogicalPlanState = {
    // Dummy query to get a statement
    val queryText = "return " + resultColumns.mkString(",")
    LogicalPlanState(
      queryText,
      startPosition = None,
      plannerName = defaultPlannerName,
      maybeStatement = Some(new CypherParser().parse(queryText)),
      maybeSemantics = None,
      maybeExtractedParams = None,
      Some(semanticTable),
      maybeUnionQuery = None,
      Some(logicalPlan),
      maybePeriodicCommit = Some(None))
  }

  private def txContext(tx: InternalTransaction): TransactionalContext = {
    val guard: Guard = null
    val queryId = 1
    val queryParameters = VirtualValues.EMPTY_MAP
    val metaData = new util.HashMap[String, AnyRef]()
    val threadToStatementContextBridge =
      dependencyResolver.provideDependency(classOf[ThreadToStatementContextBridge]).get
    val initialStatement: Statement = threadToStatementContextBridge.get()
    val threadExecutingTheQuery = Thread.currentThread()
    val activeLockCount: LongSupplier = new LongSupplier {
      override def getAsLong = 0
    }
    new Neo4jTransactionalContext(
      new GraphDatabaseCypherService(db),
      threadToStatementContextBridge,
      guard,
      threadToStatementContextBridge,
      new PropertyContainerLocker(),
      tx,
      initialStatement,
      new ExecutingQuery(
        queryId,
        ClientConnectionInfo.EMBEDDED_CONNECTION,
        "username",
        "query text",
        queryParameters,
        metaData,
        activeLockCount,
        new DefaultPageCursorTracer(),
        threadExecutingTheQuery.getId,
        threadExecutingTheQuery.getName,
        Clocks.nanoClock(),
        CpuClock.CPU_CLOCK,
        HeapAllocation.HEAP_ALLOCATION)) {
      override def close(success: Boolean): Unit = ()
    }
  }

  private def dependencyResolver = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver

  private trait TestMonitor {
    def testInvoke(obj: AnyRef)
  }

}

class InternalExecutionResultBuilder(inner: (MapValue, InternalTransaction) => InternalExecutionResult) {
  def apply(params: MapValue = VirtualValues.EMPTY_MAP, tx: InternalTransaction):
  InternalExecutionResult =
    inner.apply(params, tx)
}
