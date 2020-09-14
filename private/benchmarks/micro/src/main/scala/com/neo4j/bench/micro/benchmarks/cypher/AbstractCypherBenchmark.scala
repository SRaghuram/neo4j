/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.io.IOException
import java.util
import java.util.function.LongSupplier

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.Plans
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.RelationshipDefinition
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.EnterpriseRuntimeFactory
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.Label
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.AuthToken
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.KernelTransactionFactory
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.monitoring.Monitors
import org.neo4j.resources.CpuClock
import org.neo4j.scheduler.JobScheduler
import org.neo4j.time.Clocks
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import org.openjdk.jmh.infra.Blackhole

import scala.collection.mutable

abstract class AbstractCypherBenchmark extends BaseDatabaseBenchmark {
  private val defaultPlannerName: PlannerName = CostBasedPlannerName.default
  private val solveds = new Solveds
  private val cardinalities = new Cardinalities
  private val providedOrders = new ProvidedOrders
  private val leveragedOrders = new LeveragedOrders
  val users: mutable.Map[String, LoginContext] = mutable.Map[String, LoginContext]()

  class CountSubscriber(bh: Blackhole) extends QuerySubscriberAdapter {
    var count: Int = 0

    override def onRecord(): Unit = {
      count += 1
    }

    override def onField(offset: Int, value: AnyValue): Unit = {
      bh.consume(value)
    }
  }

  override def benchmarkGroup = "Cypher"

  override def isThreadSafe = false

  def getLogicalPlanAndSemanticTable(planContext: PlanContext): (LogicalPlan, SemanticTable, List[String])

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    val authManager = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[AuthManager])

    val labels: Array[Label] = config.labels()
    val nodeProperties: Array[PropertyDefinition] = config.nodeProperties()
    val relTypes: Array[RelationshipDefinition] = config.outRelationships()
    val relProperties: Array[PropertyDefinition] = config.relationshipProperties()

    try {
      // Role with explicit privileges to read everything in the graph
      systemDb().executeTransactionally("CREATE ROLE RoleWithGrants IF NOT EXISTS")
      systemDb().executeTransactionally("GRANT ACCESS ON DATABASE * TO RoleWithGrants")
      labels.foreach { label =>
        systemDb().executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES ${label.name()} TO RoleWithGrants")
        systemDb().executeTransactionally(s"GRANT CREATE ON GRAPH * NODES ${label.name()} TO RoleWithGrants")
        nodeProperties.foreach(p => {
          systemDb().executeTransactionally(s"GRANT READ {${p.key()}} ON GRAPH * NODES ${label.name()} TO RoleWithGrants")
          systemDb().executeTransactionally(s"GRANT SET PROPERTY {${p.key()}} ON GRAPH * NODES ${label.name()} TO RoleWithGrants")
        })
        if (nodeProperties.isEmpty) {
          systemDb().executeTransactionally(s"GRANT READ {*} ON GRAPH * NODES ${label.name()} TO RoleWithGrants")
          systemDb().executeTransactionally(s"GRANT SET PROPERTY {*} ON GRAPH * NODES ${label.name()} TO RoleWithGrants")
        }
      }
      if (labels.isEmpty) {
        systemDb().executeTransactionally("GRANT TRAVERSE ON GRAPH * NODES * TO RoleWithGrants")
        systemDb().executeTransactionally(s"GRANT CREATE ON GRAPH * NODES * TO RoleWithGrants")
        nodeProperties.foreach(p => {
          systemDb().executeTransactionally(s"GRANT READ {${p.key()}} ON GRAPH * NODES * TO RoleWithGrants")
          systemDb().executeTransactionally(s"GRANT SET PROPERTY {${p.key()}} ON GRAPH * NODES * TO RoleWithGrants")
        })
        if (nodeProperties.isEmpty) {
          systemDb().executeTransactionally(s"GRANT READ {*} ON GRAPH * NODES * TO RoleWithGrants")
          systemDb().executeTransactionally(s"GRANT SET PROPERTY {*} ON GRAPH * NODES * TO RoleWithGrants")
        }
      }
      relTypes.foreach { relDefinition =>
        val relType = relDefinition.`type`().name()
        systemDb().executeTransactionally(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS $relType TO RoleWithGrants")
        relProperties.foreach(p => systemDb().executeTransactionally(s"GRANT READ {${p.key()}} ON GRAPH * RELATIONSHIPS $relType TO RoleWithGrants"))
      }

      // Role that denies unused graph elements
      systemDb().executeTransactionally("CREATE ROLE RoleWithDenies IF NOT EXISTS")
      systemDb().executeTransactionally("GRANT ACCESS ON DATABASE * TO RoleWithDenies")
      systemDb().executeTransactionally("DENY TRAVERSE ON GRAPH * ELEMENTS DENIED TO RoleWithDenies")
      systemDb().executeTransactionally("DENY CREATE ON GRAPH * ELEMENTS DENIED TO RoleWithDenies")
      systemDb().executeTransactionally("DENY READ {deniedProp} ON GRAPH * ELEMENTS DENIED TO RoleWithDenies")
      systemDb().executeTransactionally("DENY SET PROPERTY {deniedProp} ON GRAPH * ELEMENTS DENIED TO RoleWithDenies")

      // User with grants
      systemDb().executeTransactionally("CREATE USER userWithGrants IF NOT EXISTS SET PASSWORD 'abc123' CHANGE NOT REQUIRED")
      systemDb().executeTransactionally("GRANT ROLE RoleWithGrants TO userWithGrants")

      // User with grants and denies
      systemDb().executeTransactionally("CREATE USER userWithDenies IF NOT EXISTS SET PASSWORD 'foo' CHANGE NOT REQUIRED")
      systemDb().executeTransactionally("GRANT ROLE RoleWithGrants TO userWithDenies")
      systemDb().executeTransactionally("GRANT ROLE RoleWithDenies TO userWithDenies")

      // Make sure that the label/relType/propertyKey that is denied exists in the database otherwise the denies will have no effect
      db().executeTransactionally("CALL db.createLabel('DENIED')")
      db().executeTransactionally("CALL db.createRelationshipType('DENIED')")
      db().executeTransactionally("CALL db.createProperty('deniedProp')")

      users += ("white" -> authManager.login(AuthToken.newBasicAuthToken("userWithGrants", "abc123")))
      users += ("black" -> authManager.login(AuthToken.newBasicAuthToken("userWithDenies", "foo")))
      users += ("full" -> LoginContext.AUTH_DISABLED)
    } catch {
      case e@(_: IOException | _: InvalidArgumentsException | _: InvalidAuthTokenException) =>
        throw new RuntimeException(e.getMessage, e)
    }
  }

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
    beginInternalTransaction(SecurityContext.AUTH_DISABLED)

  def beginInternalTransaction(loginContext: LoginContext): InternalTransaction =
    new GraphDatabaseCypherService(db).beginTransaction(Type.EXPLICIT, loginContext)

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
        case Pipelined => CypherRuntimeOption.pipelined
        case Parallel => CypherRuntimeOption.parallel
        case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntime")
      }

    val tx: InternalTransaction = beginInternalTransaction()
    try {
      val transactionalContext: TransactionalContext = txContext(tx)
      val planContext: PlanContext = getPlanContext(transactionalContext)
      val schemaRead = transactionalContext.kernelTransaction().schemaRead()
      val cursors = dependencyResolver.resolveDependency(classOf[Kernel]).cursors()
      val lifeSupport = dependencyResolver.resolveDependency( classOf[Database] ).getLife
      val workerManager = dependencyResolver.resolveDependency( classOf[WorkerManagement] )
      val runtimeContext = getContext(cypherRuntime, planContext, useCompiledExpressions, schemaRead, cursors, lifeSupport, workerManager)
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
                         lifeSupport             : LifeSupport,
                         workerManager           : WorkerManagement,
                         materializedEntitiesMode: Boolean = false): EnterpriseRuntimeContext =
    ContextHelper.create(
      planContext = planContext,
      debugOptions = cypherRuntime.debugOptions,
      useCompiledExpressions = useCompiledExpressions,
      jobScheduler = jobScheduler,
      schemaRead = schemaRead,
      cursors = cursors,
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
    new TransactionBoundQueryContext(transactionalContextWrapper(txContext(tx)), new ResourceManager)(searchMonitor)
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
      providedOrders,
      leveragedOrders,
      hasLoadCSV = false,
      Option.empty,
      Plans.IdGen)
  }

  private def txContext(tx: InternalTransaction): TransactionalContext = {
    val queryId = 1
    val queryParameters = VirtualValues.EMPTY_MAP
    val metaData = new util.HashMap[String, AnyRef]()
    val transactionFactory = dependencyResolver.provideDependency(classOf[KernelTransactionFactory]).get
    val databaseId = db.asInstanceOf[GraphDatabaseAPI].databaseId()
    val initialStatement: KernelStatement = tx.kernelTransaction().acquireStatement().asInstanceOf[KernelStatement]
    val threadExecutingTheQuery = Thread.currentThread()
    val emptySupplier: LongSupplier = new LongSupplier {
      override def getAsLong = 0
    }
    new Neo4jTransactionalContext(
      new GraphDatabaseCypherService(db),
      tx,
      initialStatement,
      new ExecutingQuery(queryId,
        ClientConnectionInfo.EMBEDDED_CONNECTION,
        databaseId,
        "username",
        "query text",
        queryParameters,
        metaData,
        emptySupplier,
        emptySupplier,
        emptySupplier,
        threadExecutingTheQuery.getId,
        threadExecutingTheQuery.getName,
        Clocks.nanoClock(),
        CpuClock.CPU_CLOCK,
        true),
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

    executionPlan.run(queryContext, NormalMode, params, prePopulateResults = false, input = NoInput, subscriber)
  }
}


class StubProvidedOrders extends ProvidedOrders {
  override def set(id: Id, t: ProvidedOrder): Unit = {}

  override def isDefinedAt(id: Id): Boolean = true

  override def get(id: Id): ProvidedOrder = ProvidedOrder.empty

  override def copy(from: Id, to: Id): Unit = {}
}
