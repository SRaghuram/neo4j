/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util
import java.util.function.LongSupplier

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, ProvidedOrder}
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.planner.v3_5.spi._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundPlanContext, TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.frontend.PlannerName
import org.neo4j.cypher.internal.v3_5.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.parser.CypherParser
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, LabelId, RelTypeId, Selectivity}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, EnterpriseRuntimeFactory}
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.{Kernel, Transaction}
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContext, TransactionalContext}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.resources.{CpuClock, HeapAllocation}
import org.neo4j.scheduler.JobScheduler
import org.neo4j.time.Clocks
import org.neo4j.values.virtual.{MapValue, VirtualValues}
import org.openjdk.jmh.infra.Blackhole

abstract class AbstractCypherBenchmark extends BaseDatabaseBenchmark {
  private val defaultPlannerName: PlannerName = CostBasedPlannerName.default
  private val solveds = new Solveds
  private val cardinalities = new Cardinalities

  class CountVisitor(bh: Blackhole) extends QueryResultVisitor[Exception] {
    var count: Int = 0

    override def visit(row: Record): Boolean = {
      count += 1
      bh.consume(row)
      true
    }
  }

  override def benchmarkGroup = "Cypher"

  override def isThreadSafe = false

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
    new GraphDatabaseCypherService(db).beginTransaction(Transaction.Type.explicit, SecurityContext.AUTH_DISABLED)

  private def solve(logicalPlan: LogicalPlan) {
    solveds.set(logicalPlan.id, PlannerQuery.empty)
    cardinalities.set(logicalPlan.id, 0.0)
    logicalPlan.lhs.foreach(solve)
    logicalPlan.rhs.foreach(solve)
  }

  def buildPlan(cypherRuntime: CypherRuntime, useCompiledExpressions: Boolean = false): ExecutablePlan = {
    def cypherRuntimeOption(cypherRuntime: CypherRuntime): CypherRuntimeOption =
      cypherRuntime match {
        case Interpreted            => CypherRuntimeOption.interpreted
        case EnterpriseInterpreted  => CypherRuntimeOption.slotted
        case CompiledByteCode       => CypherRuntimeOption.compiled
        case CompiledSourceCode     => CypherRuntimeOption.compiled
        case Morsel                 => CypherRuntimeOption.morsel
        case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntime")
      }

    val tx: InternalTransaction = beginInternalTransaction()
    try {
      val planContext: PlanContext = getPlanContext(tx)
      val runtimeContext = getContext(cypherRuntime, planContext, useCompiledExpressions)
      val (logicalPlan, semanticTable, resultColumns) = getLogicalPlanAndSemanticTable(planContext)
      solve(logicalPlan)
      val compilationStateBefore = getCompilationState(logicalPlan, semanticTable, resultColumns)
      val runtime = EnterpriseRuntimeFactory.getRuntime(cypherRuntimeOption(cypherRuntime), disallowFallback = true)
      val executionPlan = runtime.compileToExecutable(compilationStateBefore, runtimeContext)

      ExecutablePlan(executionPlan, tx => newQueryContext(tx))
    }
    finally {
      tx.close()
    }
  }

  private def getContext(cypherRuntime: CypherRuntime, planContext: PlanContext, useCompiledExpressions: Boolean = true): EnterpriseRuntimeContext = ContextHelper.create(
    codeStructure = GeneratedQueryStructure,
    planContext = planContext,
    debugOptions = cypherRuntime.debugOptions,
    useCompiledExpressions = useCompiledExpressions,
    jobScheduler = jobScheduler)

  private def getPlanContext(tx: InternalTransaction): PlanContext =
    new TransactionBoundPlanContext(
      transactionalContextWrapper(tx),
      devNullLogger,
      new DummyGraphStatistics())

  private def newQueryContext(tx: InternalTransaction): QueryContext = {
    val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
    new TransactionBoundQueryContext(transactionalContextWrapper(tx))(searchMonitor)
  }

  private def jobScheduler = dependencyResolver.resolveDependency(classOf[JobScheduler])

  private def kernelMonitors = dependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])

  private def transactionalContextWrapper(tx: InternalTransaction) = TransactionalContextWrapper(txContext(tx))

  private def getCompilationState(logicalPlan: LogicalPlan, semanticTable: SemanticTable, resultColumns: List[String]): LogicalPlanState = {
    // Dummy query to get a statement
    val queryText = "return " + resultColumns.mkString(",")
    LogicalPlanState(
      queryText,
      startPosition = None,
      plannerName = defaultPlannerName,
      new PlanningAttributes(solveds, cardinalities, new StubProvidedOrders()),
      maybeStatement = Some(new CypherParser().parse(queryText)),
      maybeSemantics = None,
      maybeExtractedParams = None,
      Some(semanticTable),
      maybeUnionQuery = None,
      Some(logicalPlan),
      maybePeriodicCommit = Some(None))
  }

  private def txContext(tx: InternalTransaction): TransactionalContext = {
    val queryId = 1
    val queryParameters = VirtualValues.EMPTY_MAP
    val metaData = new util.HashMap[String, AnyRef]()
    val threadToStatementContextBridge =
      dependencyResolver.provideDependency(classOf[ThreadToStatementContextBridge]).get
    val kernel = dependencyResolver.resolveDependency(classOf[Kernel])
    val initialStatement: Statement = threadToStatementContextBridge.get()
    val threadExecutingTheQuery = Thread.currentThread()
    val activeLockCount: LongSupplier = new LongSupplier {
      override def getAsLong = 0
    }
    new Neo4jTransactionalContext(
      new GraphDatabaseCypherService(db),
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
        HeapAllocation.HEAP_ALLOCATION),
      kernel) {
      override def close(success: Boolean): Unit = ()
    }
  }

  private def dependencyResolver = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver

  private trait TestMonitor {
    def testInvoke(obj: AnyRef)
  }

  private class DummyGraphStatistics extends GraphStatistics {
    override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = ???

    override def nodesAllCardinality(): Cardinality = ???

    override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = ???

    override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] = ???

    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = ???
  }
}

case class ExecutablePlan(executionPlan: ExecutionPlan, newQueryContext: InternalTransaction => QueryContext) {
  def execute(params: MapValue = VirtualValues.EMPTY_MAP, tx: InternalTransaction): RuntimeResult = {
    val queryContext = newQueryContext(tx)

    executionPlan.run(queryContext, doProfile = false, params)
  }
}


class StubProvidedOrders extends ProvidedOrders {
  override def set(id: Id, t: ProvidedOrder): Unit = {}

  override def isDefinedAt(id: Id): Boolean = true

  override def get(id: Id): ProvidedOrder = ProvidedOrder.empty

  override def copy(from: Id, to: Id): Unit = {}
}
