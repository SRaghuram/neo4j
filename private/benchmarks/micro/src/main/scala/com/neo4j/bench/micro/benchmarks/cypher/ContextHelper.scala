/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.time.Clock
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.CypherMorselRuntimeSchedulerOption
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.v4_0.frontend.phases.{CompilationPhaseTracer, InternalNotificationLogger, devNullLogger}
import org.neo4j.cypher.internal.v4_0.util.{CypherException, InputPosition}
import org.neo4j.cypher.internal.{CypherRuntimeConfiguration, EnterpriseRuntimeContext, NoSchedulerTracing, RuntimeEnvironment}
import org.neo4j.internal.kernel.api.{CursorFactory, SchemaRead}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration.Duration

object ContextHelper extends MockitoSugar {

  private val runtimeConfig = CypherRuntimeConfiguration(
    workers = Runtime.getRuntime.availableProcessors(),
    morselSize = 10000,
    schedulerTracing = NoSchedulerTracing,
    waitTimeout = Duration(3000, TimeUnit.MILLISECONDS),
    scheduler = CypherMorselRuntimeSchedulerOption.default,
    lenientCreateRelationship = false
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
             txBridge: ThreadToStatementContextBridge): EnterpriseRuntimeContext = {
    EnterpriseRuntimeContext(
      planContext,
      schemaRead,
      codeStructure,
      NullLog.getInstance(),
      clock,
      debugOptions,
      runtimeConfig,
      runtimeEnvironment = RuntimeEnvironment.of(runtimeConfig, jobScheduler, cursors, txBridge),
      compileExpressions = useCompiledExpressions)
  }
}
