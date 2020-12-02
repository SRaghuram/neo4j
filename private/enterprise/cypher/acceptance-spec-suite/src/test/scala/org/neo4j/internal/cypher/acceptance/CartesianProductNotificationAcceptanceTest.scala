/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.Clock

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlanner
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.CypherPlannerFactory
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.defaultUpdateStrategy
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_3
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.PlannerContextCreator
import org.neo4j.cypher.internal.compiler.planner.logical.CachedMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.rewriting.rewriters.GeneratingNamer
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.Statement
import org.neo4j.values.virtual.MapValue

class CartesianProductNotificationAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {
  var logger: InternalNotificationLogger = _
  var compiler: CypherPlanner[PlannerContext] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    logger = mock[InternalNotificationLogger]
    compiler = createCompiler()
  }

  test("should warn when disconnected patterns") {
    //when
    runQuery("MATCH (a)-->(b), (c)-->(d) RETURN *")

    //then
    verify(logger).log(CartesianProductNotification(InputPosition(0, 1, 1), Set("c", "d")))
  }

  test("should not warn when connected patterns") {
    //when
    runQuery("MATCH (a)-->(b), (a)-->(c) RETURN *")

    //then
    verify(logger, never).log(any())
  }

  test("should warn when one disconnected pattern in otherwise connected pattern") {
    //when
    runQuery("MATCH (a)-->(b), (b)-->(c), (x)-->(y), (c)-->(d), (d)-->(e) RETURN *")

    //then
    verify(logger).log(CartesianProductNotification(InputPosition(0, 1, 1), Set("x", "y")))
  }

  test("should not warn when disconnected patterns in multiple match clauses") {
    //when
    runQuery("MATCH (a)-->(b) MATCH (c)-->(d) RETURN *")

    //then
    verify(logger, never).log(any())
  }

  test("this query does not contain a cartesian product") {
    //when
    runQuery(
      """MATCH (p)-[r1]-(m),
        |(m)-[r2]-(d), (d)-[r3]-(m2)
        |RETURN DISTINCT d""".stripMargin)

    //then
    verify(logger, never).log(any())
  }

  private def runQuery(query: String) = {
    graph.withTx( tx => {
      val tracer = CompilationPhaseTracer.NO_TRACING
      val innerVariableNamer = new GeneratingNamer
      val parsed = compiler.parseQuery(query, query, logger, IDPPlannerName.name, CypherDebugOptions.default, None, tracer, innerVariableNamer, MapValue.EMPTY, compatibilityMode = Compatibility4_3)
      val kernelTransaction = tx.kernelTransaction()
      val statement = kernelTransaction.acquireStatement()
      val context = PlannerContextCreator.create(tracer, logger, planContext(kernelTransaction, statement), parsed.queryText, CypherDebugOptions.default, ExecutionModel.default,
                                                 None, monitors, metricsFactory, createQueryGraphSolver(), configuration, defaultUpdateStrategy, Clock.systemUTC(), new SequentialIdGen(),
                                                 simpleExpressionEvaluator, innerVariableNamer, MapValue.EMPTY)

      try {
        val normalized = compiler.normalizeQuery(parsed, context)
        compiler.planPreparedQuery(normalized, context)
      }
      finally {
        statement.close()
      }
    } )
  }

  private val configuration = CypherPlannerConfiguration(
    queryCacheSize = 128,
    statsDivergenceCalculator = StatsDivergenceCalculator.divergenceNoDecayCalculator(0.5, 1000),
    useErrorsOverWarnings = false,
    idpMaxTableSize = 128,
    idpIterationDuration = 1000,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = true,
    legacyCsvQuoteEscaping = false,
    csvBufferSize = CSVResources.DEFAULT_BUFFER_SIZE,
    nonIndexedLabelWarningThreshold = 10000L,
    planSystemCommands = false,
    useJavaCCParser = true,
    pipelinedBatchSizeSmall = 128,
    pipelinedBatchSizeBig = 1024,
  )
  private lazy val monitors = WrappedMonitors(kernelMonitors)
  private val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
  private def createCompiler(): CypherPlanner[PlannerContext] = {

    new CypherPlannerFactory().costBasedCompiler(
      configuration,
      Clock.systemUTC(),
      monitors,
      updateStrategy = None,
      contextCreator = PlannerContextCreator
    )
  }

  private def planContext(transaction: KernelTransaction, statement: Statement): PlanContext = {
    val tc = mock[TransactionalContextWrapper]
    when(tc.dataRead).thenReturn(transaction.dataRead())
    when(tc.graph).thenReturn(graph)
    TransactionBoundPlanContext(tc, devNullLogger, null)
  }

  private def createQueryGraphSolver(): IDPQueryGraphSolver = {
    val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
    val solverConfig = new ConfigurableIDPSolverConfig(
      maxTableSize = configuration.idpMaxTableSize,
      iterationDurationLimit = configuration.idpIterationDuration
    )
    val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
    IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
  }
}
