/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.time.Clock
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.compatibility.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, StatsDivergenceCalculator}
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.v3_5.frontend.phases.{CompilationPhaseTracer, InternalNotificationLogger, devNullLogger}
import org.neo4j.cypher.internal.v3_5.util.{CypherException, InputPosition}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, RuntimeEnvironment}
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration.Duration

object ContextHelper extends MockitoSugar {
  private val plannerConfig = CypherPlannerConfiguration(
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
    planWithMinimumCardinalityEstimates = false,
    lenientCreateRelationship = false
  )

  private val runtimeConfig = CypherRuntimeConfiguration(
    workers = Runtime.getRuntime.availableProcessors(),
    morselSize = 10000,
    doSchedulerTracing = false,
    waitTimeout = Duration(3000, TimeUnit.MILLISECONDS)
  )

  def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null,
             tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING,
             notificationLogger: InternalNotificationLogger = devNullLogger,
             planContext: PlanContext,
             debugOptions: Set[String] = Set.empty,
             clock: Clock = Clock.systemUTC(),
             codeStructure: CodeStructure[GeneratedQuery] = mock[CodeStructure[GeneratedQuery]],
             useCompiledExpressions: Boolean = false,
             jobScheduler: JobScheduler): EnterpriseRuntimeContext = {
                             EnterpriseRuntimeContext(
                               planContext,
                               readOnly = true,
                               codeStructure,
                               NullLog.getInstance(),
                               clock,
                               debugOptions,
                               plannerConfig,
                               runtimeEnvironment = RuntimeEnvironment(runtimeConfig, jobScheduler),
                               compileExpressions = useCompiledExpressions)
  }
}
