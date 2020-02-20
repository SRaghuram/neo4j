/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.time.Clock

import com.neo4j.bench.micro.benchmarks.cypher.ContextHelper.mock
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.CypherOperatorEngineOption
import org.neo4j.cypher.internal.ConfigMemoryTrackingController
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.NoSchedulerTracing
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler
import org.scalatest.mock.MockitoSugar

object ContextHelper extends MockitoSugar {

  // Always use the big morsel size here, since the generated plans do not have cardinality information
  // and we would otherwise end up using the small morsel size even though the micro benchmarks
  // typically have lots of result rows.
  private val morselSize = GraphDatabaseSettings.cypher_pipelined_batch_size_big.defaultValue()

  private val runtimeConfig = CypherRuntimeConfiguration(
    pipelinedBatchSizeSmall = morselSize,
    pipelinedBatchSizeBig = morselSize,
    schedulerTracing = NoSchedulerTracing,
    lenientCreateRelationship = false,
    memoryTrackingController = new ConfigMemoryTrackingController(Config.defaults()),
    enableMonitors = false
  )

  def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null,
             tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING,
             notificationLogger: InternalNotificationLogger = devNullLogger,
             planContext: PlanContext,
             debugOptions: Set[String] = Set.empty,
             clock: Clock = Clock.systemUTC(),
             codeStructure: CodeStructure[GeneratedQuery] = mock[CodeStructure[GeneratedQuery]],
             useCompiledExpressions: Boolean = false,
             jobScheduler: JobScheduler,
             schemaRead: SchemaRead,
             cursors: CursorFactory,
             lifeSupport: LifeSupport,
             workerManager: WorkerManagement,
             materializedEntitiesMode: Boolean = false): EnterpriseRuntimeContext = {
    EnterpriseRuntimeContext(
      planContext,
      schemaRead,
      codeStructure,
      NullLog.getInstance(),
      clock,
      debugOptions,
      runtimeConfig,
      runtimeEnvironment = RuntimeEnvironment.of(runtimeConfig, jobScheduler, cursors, lifeSupport, workerManager),
      compileExpressions = useCompiledExpressions,
      materializedEntitiesMode = materializedEntitiesMode,
      operatorEngine = CypherOperatorEngineOption.compiled,
      interpretedPipesFallback = CypherInterpretedPipesFallbackOption(GraphDatabaseSettings.cypher_pipelined_interpreted_pipes_fallback.defaultValue().toString),
    )
  }
}
