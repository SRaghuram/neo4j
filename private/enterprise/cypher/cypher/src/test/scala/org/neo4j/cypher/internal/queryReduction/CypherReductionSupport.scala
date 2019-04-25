/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility.v4_0.WrappedMonitors
import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.planner.logical.idp.{IDPQueryGraphSolver, IDPQueryGraphSolverMonitor, SingleComponentPlanner, cartesianProductsOrValueJoins}
import org.neo4j.cypher.internal.compiler.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.{CypherPlanner, CypherPlannerConfiguration, StatsDivergenceCalculator, defaultUpdateStrategy}
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.planner.spi.{IDPPlannerName, PlanContext, PlannerNameFor, PlanningAttributes}
import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticFeature, SemanticState}
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.GeneratingNamer
import org.neo4j.cypher.internal.v4_0.rewriting.{Deprecations, RewriterStepSequencer}
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.{CypherRuntimeOption, GraphIcing}
import org.neo4j.dbms.database.DatabaseManagementService
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber.NOT_A_SUBSCRIBER
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, TransactionalContextFactory}
import org.neo4j.logging.NullLog
import org.neo4j.monitoring.Monitors
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.util.Try

object CypherReductionSupport {
  private val stepSequencer = RewriterStepSequencer.newPlain _
  private val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
  private val config = CypherPlannerConfiguration(
    queryCacheSize = 0,
    statsDivergenceCalculator = StatsDivergenceCalculator.divergenceNoDecayCalculator(0, 0),
    useErrorsOverWarnings = false,
    idpMaxTableSize = 128,
    idpIterationDuration = 1000,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = false,
    legacyCsvQuoteEscaping = false,
    csvBufferSize = CSVResources.DEFAULT_BUFFER_SIZE,
    nonIndexedLabelWarningThreshold = 0,
    planWithMinimumCardinalityEstimates = true)
  private val kernelMonitors = new Monitors
  private val compiler = CypherPlanner(WrappedMonitors(kernelMonitors), stepSequencer, metricsFactory, config, defaultUpdateStrategy,
    MasterCompiler.CLOCK, PlannerContextCreator)

  private val monitor = kernelMonitors.newMonitor(classOf[IDPQueryGraphSolverMonitor])
  private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
  private val singleComponentPlanner = SingleComponentPlanner(monitor)
  private val queryGraphSolver = IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

  val prettifier = Prettifier(ExpressionStringifier())
}

/**
  * Do not mixin GraphDatabaseTestSupport when using this object.
  */
trait CypherReductionSupport extends CypherTestSupport with GraphIcing {
  self: CypherFunSuite  =>

  private var managementService: DatabaseManagementService = _
  private var graph: GraphDatabaseCypherService = _
  private var contextFactory: TransactionalContextFactory = _

  override protected def initTest() {
    super.initTest()
    managementService = new TestDatabaseManagementServiceBuilder().newImpermanentService()
    graph = new GraphDatabaseCypherService(managementService.database(DEFAULT_DATABASE_NAME))
    contextFactory = Neo4jTransactionalContextFactory.create(graph)
  }

  override protected def stopTest() {
    try {
      super.stopTest()
    }
    finally {
      if (graph != null) managementService.shutdown()
    }
  }

  private val rewriting = PreparatoryRewriting(Deprecations.V1) andThen
    SemanticAnalysis(warn = true, SemanticFeature.Cypher9Comparability).adds(BaseContains[SemanticState])

  def evaluate(query: String, executeBefore: Option[String] = None, enterprise: Boolean = false): RewindableExecutionResult = {
    val parsingBaseState = queryToParsingBaseState(query, enterprise)
    val statement = parsingBaseState.statement()
    produceResult(query, statement, parsingBaseState, executeBefore, enterprise)
  }

  def reduceQuery(query: String, executeBefore: Option[String] = None, enterprise: Boolean = false)(test: Oracle[Try[RewindableExecutionResult]]): String = {
    val oracle: Oracle[Try[(String, RewindableExecutionResult)]] = tryTuple => {
      val tryResult = tryTuple.map(_._2)
      test(tryResult)
    }
    reduceQueryWithCurrentQueryText(query, executeBefore, enterprise)(oracle)
  }

  def reduceQueryWithCurrentQueryText(query: String, executeBefore: Option[String] = None, enterprise: Boolean = false)(test: Oracle[Try[(String, RewindableExecutionResult)]]): String = {
    val parsingBaseState = queryToParsingBaseState(query, enterprise)
    val statement = parsingBaseState.statement()

    val oracle: Oracle[Statement] = currentStatement => {
      // Actual query
      val currentlyRunQuery = CypherReductionSupport.prettifier.asString(currentStatement)
      val tryResults = Try((currentlyRunQuery, produceResult(query, currentStatement, parsingBaseState, executeBefore, enterprise)))
      val testRes = test(tryResults)
      testRes
    }

    val smallerStatement = GTRStar(new StatementGTRInput(statement))(oracle)
    CypherReductionSupport.prettifier.asString(smallerStatement)
  }

  private def queryToParsingBaseState(query: String, enterprise: Boolean): BaseState = {
    val startState = LogicalPlanState(query, None, PlannerNameFor(IDPPlannerName.name), PlanningAttributes(new Solveds, new Cardinalities, new ProvidedOrders))
    val parsingContext = createContext(query, CypherReductionSupport.metricsFactory, CypherReductionSupport.config, null, null, enterprise)
    CompilationPhases.parsing(CypherReductionSupport.stepSequencer).transform(startState, parsingContext)
  }

  private def produceResult(query: String,
                            statement: Statement,
                            parsingBaseState: BaseState,
                            executeBefore: Option[String],
                            enterprise: Boolean): RewindableExecutionResult = {
    val explicitTx = graph.beginTransaction(Transaction.Type.explicit, LoginContext.AUTH_DISABLED)
    val implicitTx = graph.beginTransaction(Transaction.Type.`implicit`, LoginContext.AUTH_DISABLED)
    try {
      executeBefore match {
        case None =>
        case Some(setupQuery) =>
          val setupBS = queryToParsingBaseState(setupQuery, enterprise)
          val setupStm = setupBS.statement()
          executeInTx(setupQuery, setupStm, setupBS, implicitTx, enterprise)
      }
      executeInTx(query, statement, parsingBaseState, implicitTx, enterprise)
    } finally {
      explicitTx.failure()
      explicitTx.close()
    }
  }

  private def executeInTx(query: String,
                          statement: Statement,
                          parsingBaseState: BaseState,
                          implicitTx: InternalTransaction,
                          enterprise: Boolean
                         ): RewindableExecutionResult = {
    val neo4jtxContext = contextFactory.newContext(implicitTx, query, EMPTY_MAP)
    val txContextWrapper = TransactionalContextWrapper(neo4jtxContext)
    val planContext = TransactionBoundPlanContext(txContextWrapper, devNullLogger)

    var baseState = parsingBaseState.withStatement(statement)
    val planningContext = createContext(query, CypherReductionSupport.metricsFactory, CypherReductionSupport.config, planContext, CypherReductionSupport.queryGraphSolver, enterprise)


    baseState = rewriting.transform(baseState, planningContext)

    val logicalPlanState = CypherReductionSupport.compiler.planPreparedQuery(baseState, planningContext)
    val readOnly = logicalPlanState.planningAttributes.solveds(logicalPlanState.logicalPlan.id).readOnly

    val runtime = CommunityRuntimeFactory.getRuntime(CypherRuntimeOption.default, planningContext.config.useErrorsOverWarnings)

    val runtimeConfig = CypherConfiguration.fromConfig(Config.defaults()).toCypherRuntimeConfiguration
    val runtimeContextCreator = if (enterprise)
      EnterpriseRuntimeContextCreator(
        GeneratedQueryStructure,
        NullLog.getInstance(),
        runtimeConfig,
        morselRuntimeState = null)
     else
      CommunityRuntimeContextCreator(runtimeConfig)

    val runtimeContext = runtimeContextCreator.create(planContext,
                                                      txContextWrapper.kernelTransaction.schemaRead(),
                                                      MasterCompiler.CLOCK,
                                                      Set(),
                                                      enterprise)

    val logicalQuery = LogicalQuery(logicalPlanState.logicalPlan,
                                    logicalPlanState.queryText,
                                    readOnly,
                                    logicalPlanState.statement().returnColumns.toArray,
                                    logicalPlanState.semanticTable(),
                                    logicalPlanState.planningAttributes.cardinalities,
                                    logicalPlanState.hasLoadCSV,
                                    logicalPlanState.maybePeriodicCommit.flatMap(_.map(x => PeriodicCommitInfo(x.batchSize))))

    val executionPlan = runtime.compileToExecutable(logicalQuery, runtimeContext)

    val queryContext = new TransactionBoundQueryContext(txContextWrapper)(CypherReductionSupport.searchMonitor)

    val runtimeResult = executionPlan.run(queryContext, doProfile = false, ValueConversion.asValues(baseState.extractedParams()),
                                          prePopulateResults = false, NoInput, NOT_A_SUBSCRIBER)
    RewindableExecutionResult(runtimeResult, queryContext)
  }

  private def createContext(query: String, metricsFactory: CachedMetricsFactory,
                            config: CypherPlannerConfiguration,
                            planContext: PlanContext,
                            queryGraphSolver: IDPQueryGraphSolver, enterprise: Boolean) = {
    val logicalPlanIdGen = new SequentialIdGen()
    PlannerContextCreator.create(NO_TRACING, devNullLogger, planContext, query, Set(),
                                 None, WrappedMonitors(new Monitors), metricsFactory,
                                 queryGraphSolver, config, defaultUpdateStrategy, MasterCompiler.CLOCK,
                                 logicalPlanIdGen, null, new GeneratingNamer)
  }
}
