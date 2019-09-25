/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.time.Clock

import org.neo4j.cypher.internal._
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.{CypherInterpretedPipesFallbackOption, CypherOperatorEngineOption}
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.morsel.WorkerManagement
import org.neo4j.cypher.internal.v4_0.frontend.phases.{CompilationPhaseTracer, InternalNotificationLogger, devNullLogger}
import org.neo4j.cypher.internal.v4_0.util.{CypherException, InputPosition}
import org.neo4j.internal.kernel.api.{CursorFactory, SchemaRead}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler
import org.scalatest.mock.MockitoSugar

object ContextHelper extends MockitoSugar {

  private val runtimeConfig = CypherRuntimeConfiguration(
    workers = Runtime.getRuntime.availableProcessors(),
    morselSizeSmall = GraphDatabaseSettings.cypher_morsel_size_small.defaultValue(),
    morselSizeBig = GraphDatabaseSettings.cypher_morsel_size_big.defaultValue(),
    schedulerTracing = NoSchedulerTracing,
    lenientCreateRelationship = false,
    interpretedPipesFallback = CypherInterpretedPipesFallbackOption(GraphDatabaseSettings.cypher_morsel_use_interpreted_pipes.defaultValue().toString),
    memoryTrackingController = new ConfigMemoryTrackingController(Config.defaults())
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
             txBridge: ThreadToStatementContextBridge,
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
      runtimeEnvironment = RuntimeEnvironment.of(runtimeConfig, jobScheduler, cursors, txBridge, lifeSupport, workerManager),
      compileExpressions = useCompiledExpressions,
      materializedEntitiesMode = materializedEntitiesMode,
      operatorEngine = CypherOperatorEngineOption.compiled)
  }
}
