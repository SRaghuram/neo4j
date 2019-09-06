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
import org.neo4j.cypher.internal.ir.{SinglePlannerQuery, ProvidedOrder}
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.planner.spi._
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.morsel.WorkerManagement
import org.neo4j.cypher.internal.runtime.{NoInput, QueryContext}
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, LabelId, RelTypeId, Selectivity}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, EnterpriseRuntimeFactory, ExecutionPlan, LogicalQuery}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.{CursorFactory, Kernel, SchemaRead, Transaction}
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, ThreadToStatementContextBridge}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.KernelTransactionFactory
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContext, QuerySubscriber, QuerySubscriberAdapter, TransactionalContext}
import org.neo4j.kernel.impl.util.DefaultValueMapper
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.monitoring.Monitors
import org.neo4j.resources.{CpuClock, HeapAllocation}
import org.neo4j.scheduler.JobScheduler
import org.neo4j.time.Clocks
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{MapValue, VirtualValues}
import org.openjdk.jmh.infra.Blackhole

abstract class AbstractCypherBenchmark extends BaseDatabaseBenchmark {
  private val defaultPlannerName: PlannerName = CostBasedPlannerName.default
  private val solveds = new Solveds
  private val cardinalities = new Cardinalities

  class CountSubscriber(bh: Blackhole) extends QuerySubscriberAdapter {
    var count: Int = 0

    override def onRecord(): Unit = {
      count += 1
    }

    override def onField(value: AnyValue): Unit = {
      bh.consume(value)
    }
  }

  override def benchmarkGroup = "Cypher"

  override def isThreadSafe = false

  def getLogicalPlanAndSemanticTable(planContext: PlanContext): (LogicalPlan, SemanticTable, List[String])

  def assertExpectedRowCount(expectedRowCount: Int, visitor: CountSubscriber): Int =
    if (visitor.count != expectedRowCount) {
      val actualCount = visitor.count
      throw new RuntimeException(s"Expected $expectedRowCount results but found $actualCount")
    } else visitor.count

  def assertExpectedRowCount(minRowCount: Int, maxRowCount: Int, visitor: CountSubscriber): Int =
    if (minRowCount > visitor.count || visitor.count > maxRowCount) {
      val actualCount = visitor.count
      throw new RuntimeException(s"Expected result count in range ($minRowCount,$maxRowCount) but found $actualCount")
    } else visitor.count

  def beginInternalTransaction(): InternalTransaction =
    new GraphDatabaseCypherService(db).beginTransaction(Transaction.Type.explicit, SecurityContext.AUTH_DISABLED)

  private def solve(logicalPlan: LogicalPlan) {
    solveds.set(logicalPlan.id, SinglePlannerQuery.empty)
    cardinalities.set(logicalPlan.id, 0.0)
    logicalPlan.lhs.foreach(solve)
    logicalPlan.rhs.foreach(solve)
  }

  def buildPlan(cypherRuntime: CypherRuntime, useCompiledExpressions: Boolean = true): ExecutablePlan = {
    def cypherRuntimeOption(cypherRuntime: CypherRuntime): CypherRuntimeOption =
      cypherRuntime match {
        case Interpreted => CypherRuntimeOption.interpreted
        case Slotted => CypherRuntimeOption.slotted
        case CompiledByteCode => CypherRuntimeOption.compiled
        case CompiledSourceCode => CypherRuntimeOption.compiled
        case Morsel => CypherRuntimeOption.morsel
        case Parallel => CypherRuntimeOption.parallel
        case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntime")
      }

    val tx: InternalTransaction = beginInternalTransaction()
    try {
      val transactionalContext: TransactionalContext = txContext(tx)
      val planContext: PlanContext = getPlanContext(transactionalContext)
      val schemaRead = transactionalContext.kernelTransaction().schemaRead()
      val cursors = dependencyResolver.resolveDependency(classOf[Kernel]).cursors()
      val txBridge = dependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
      val lifeSupport = dependencyResolver.resolveDependency( classOf[Database] ).getLife
      val workerManager = dependencyResolver.resolveDependency( classOf[WorkerManagement] )
      val runtimeContext = getContext(cypherRuntime, planContext, useCompiledExpressions, schemaRead, cursors, txBridge, lifeSupport, workerManager)
      val (logicalPlan, semanticTable, resultColumns) = getLogicalPlanAndSemanticTable(planContext)
      solve(logicalPlan)
      val compilationStateBefore = getLogicalQuery(logicalPlan, semanticTable, resultColumns)
      val runtime = EnterpriseRuntimeFactory.getRuntime(cypherRuntimeOption(cypherRuntime), disallowFallback = true)
      val executionPlan = runtime.compileToExecutable(compilationStateBefore, runtimeContext)

      ExecutablePlan(executionPlan, tx => newQueryContext(tx))
    }
    finally {
      tx.close()
    }
  }

  private def getContext(cypherRuntime           : CypherRuntime,
                         planContext             : PlanContext,
                         useCompiledExpressions  : Boolean = true,
                         schemaRead              : SchemaRead,
                         cursors                 : CursorFactory,
                         txBridge                : ThreadToStatementContextBridge,
                         lifeSupport             : LifeSupport,
                         workerManager           : WorkerManagement,
                         materializedEntitiesMode: Boolean = false): EnterpriseRuntimeContext =
    ContextHelper.create(
      codeStructure = GeneratedQueryStructure,
      planContext = planContext,
      debugOptions = cypherRuntime.debugOptions,
      useCompiledExpressions = useCompiledExpressions,
      jobScheduler = jobScheduler,
      schemaRead = schemaRead,
      cursors = cursors,
      txBridge = txBridge,
      lifeSupport = lifeSupport,
      workerManager = workerManager,
      materializedEntitiesMode = materializedEntitiesMode)

  private def getPlanContext(tx: TransactionalContext): PlanContext =
    new TransactionBoundPlanContext(
      transactionalContextWrapper(tx),
      devNullLogger,
      InstrumentedGraphStatistics(new DummyGraphStatistics(), new MutableGraphStatisticsSnapshot()))

  private def newQueryContext(tx: InternalTransaction): QueryContext = {
    val searchMonitor: IndexSearchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
    new TransactionBoundQueryContext(transactionalContextWrapper(txContext(tx)))(searchMonitor)
  }

  private def jobScheduler = dependencyResolver.resolveDependency(classOf[JobScheduler])

  private def kernelMonitors = dependencyResolver.resolveDependency(classOf[Monitors])

  private def transactionalContextWrapper(txContext: TransactionalContext) = TransactionalContextWrapper(txContext)

  private def getLogicalQuery(logicalPlan: LogicalPlan, semanticTable: SemanticTable, resultColumns: List[String]): LogicalQuery = {
    // Dummy query to get a statement
    val queryText = "return " + resultColumns.mkString(",")
    LogicalQuery(
      logicalPlan,
      queryText,
      readOnly = true,
      resultColumns.toArray,
      semanticTable,
      cardinalities,
      hasLoadCSV = false,
      Option.empty)
  }

  private def txContext(tx: InternalTransaction): TransactionalContext = {
    val queryId = 1
    val queryParameters = VirtualValues.EMPTY_MAP
    val metaData = new util.HashMap[String, AnyRef]()
    val threadToStatementContextBridge =
      dependencyResolver.provideDependency(classOf[ThreadToStatementContextBridge]).get
    val transactionFactory = dependencyResolver.provideDependency(classOf[KernelTransactionFactory]).get
    val databaseId = db.asInstanceOf[GraphDatabaseAPI].databaseId()
    val initialStatement: Statement = threadToStatementContextBridge.get(databaseId)
    val threadExecutingTheQuery = Thread.currentThread()
    val activeLockCount: LongSupplier = new LongSupplier {
      override def getAsLong = 0
    }
    val proxySpi = dependencyResolver.resolveDependency(classOf[EmbeddedProxySPI])
    new Neo4jTransactionalContext(
      new GraphDatabaseCypherService(db),
      threadToStatementContextBridge,
      tx,
      initialStatement,
      new ExecutingQuery(queryId,
                         ClientConnectionInfo.EMBEDDED_CONNECTION,
                         databaseId,
                         "username",
                         "query text",
                         queryParameters,
                         metaData,
                         activeLockCount,
                         new DefaultPageCursorTracer(),
                         threadExecutingTheQuery.getId,
                         threadExecutingTheQuery.getName,
                         Clocks.nanoClock(),
                         CpuClock.CPU_CLOCK),
      new DefaultValueMapper(proxySpi),
      transactionFactory) {

      override def close(): Unit = ()
    }
  }

  private def dependencyResolver = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver

  private trait TestMonitor {
    def testInvoke(obj: AnyRef)
  }

  private class DummyGraphStatistics extends GraphStatistics {
    override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = ???

    override def nodesAllCardinality(): Cardinality = ???

    override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = ???

    override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] = ???

    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = ???
  }

}

case class ExecutablePlan(executionPlan: ExecutionPlan, newQueryContext: InternalTransaction => QueryContext) {
  def execute(params: MapValue = VirtualValues.EMPTY_MAP, tx: InternalTransaction, subscriber: QuerySubscriber): RuntimeResult = {
    val queryContext = newQueryContext(tx)

    executionPlan.run(queryContext, doProfile = false, params, prePopulateResults = false, input = NoInput, subscriber)
  }
}


class StubProvidedOrders extends ProvidedOrders {
  override def set(id: Id, t: ProvidedOrder): Unit = {}

  override def isDefinedAt(id: Id): Boolean = true

  override def get(id: Id): ProvidedOrder = ProvidedOrder.empty

  override def copy(from: Id, to: Id): Unit = {}
}
