/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.time.Clock

import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.CypherOperatorEngineOption
import org.neo4j.cypher.CypherPlannerOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.phases.Compatibility3_5
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_0
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_1
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerTracer
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.query.QueryEngineProvider.SPI
import org.neo4j.logging.Log

class EnterpriseCompilerFactory(graph: GraphDatabaseQueryService,
                                spi: SPI,
                                plannerConfig: CypherPlannerConfiguration,
                                runtimeConfig: CypherRuntimeConfiguration
                               ) extends CompilerFactory {
  /*
  One compiler is created for every Planner:Runtime:Version combination, e.g., Cost-Pipelined-3.5 & Cost-Pipelined-4.0.
  Each compiler contains a runtime instance, and each pipelined runtime instance requires a dispatcher instance.
  This ensures only one (shared) dispatcher/tracer instance is created, even when there are multiple pipelined runtime instances.
   */
  private val runtimeEnvironment: RuntimeEnvironment = {
    val resolver = graph.getDependencyResolver
    val workerManager = resolver.resolveDependency(classOf[WorkerManagement])
    RuntimeEnvironment.of(runtimeConfig, spi.jobScheduler, spi.kernel.cursors(), spi.lifeSupport, workerManager)
  }

  private val log: Log = spi.logProvider().getLog(getClass)

  override def supportsAdministrativeCommands(): Boolean = plannerConfig.planSystemCommands

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy,
                              executionEngineProvider: () => ExecutionEngine): Compiler = {

    val compatibilityMode = cypherVersion match {
      case CypherVersion.v3_5 => Compatibility3_5
      case CypherVersion.v4_0 => Compatibility4_0
      case CypherVersion.v4_1 => Compatibility4_1
    }

    val planner =
      CypherPlanner(
        plannerConfig,
        MasterCompiler.CLOCK,
        spi.monitors(),
        log,
        cypherPlanner,
        cypherUpdateStrategy,
        LastCommittedTxIdProvider(graph),
        compatibilityMode)

    val runtime = if (plannerConfig.planSystemCommands) {
      cypherVersion match {
        case CypherVersion.v3_5 => throw new SyntaxException("Commands towards system database are not supported in this Cypher version.")
        case _ => EnterpriseAdministrationCommandRuntime(executionEngineProvider(), graph.getDependencyResolver)
      }
    } else {
      EnterpriseRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings)
    }

    CypherCurrentCompiler(
      planner,
      runtime,
      EnterpriseRuntimeContextManager(GeneratedQueryStructure, log, runtimeConfig, runtimeEnvironment),
      spi.monitors())
  }
}

/**
 * Enterprise runtime context. Enriches the community runtime context with infrastructure needed for
 * query compilation and parallel execution.
 */
case class EnterpriseRuntimeContext(tokenContext: TokenContext,
                                    schemaRead: SchemaRead,
                                    codeStructure: CodeStructure[GeneratedQuery],
                                    log: Log,
                                    clock: Clock,
                                    debugOptions: Set[String],
                                    config: CypherRuntimeConfiguration,
                                    runtimeEnvironment: RuntimeEnvironment,
                                    compileExpressions: Boolean,
                                    materializedEntitiesMode: Boolean,
                                    operatorEngine: CypherOperatorEngineOption,
                                    interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
                                    cachingExpressionCompilerTracer: CachingExpressionCompilerTracer) extends RuntimeContext

/**
 * Manager of EnterpriseRuntimeContexts.
 */
case class EnterpriseRuntimeContextManager(codeStructure: CodeStructure[GeneratedQuery],
                                           log: Log,
                                           config: CypherRuntimeConfiguration,
                                           runtimeEnvironment: RuntimeEnvironment)
  extends RuntimeContextManager[EnterpriseRuntimeContext] {

  override def create(tokenContext: TokenContext,
                      schemaRead: SchemaRead,
                      clock: Clock,
                      debugOptions: Set[String],
                      compileExpressions: Boolean,
                      materializedEntitiesMode: Boolean,
                      operatorEngine: CypherOperatorEngineOption,
                      interpretedPipesFallback: CypherInterpretedPipesFallbackOption): EnterpriseRuntimeContext =
    EnterpriseRuntimeContext(tokenContext,
      schemaRead,
      codeStructure,
      log,
      clock,
      debugOptions,
      config,
      runtimeEnvironment,
      compileExpressions,
      materializedEntitiesMode,
      operatorEngine,
      interpretedPipesFallback,
      CachingExpressionCompilerTracer.NONE)

  override def assertAllReleased(): Unit = {
    // This is for test assertions only, and should run on the parallel executor.
    runtimeEnvironment.getQueryExecutor(parallelExecution = true).assertAllReleased()
  }
}
