/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_4.Cypher3_4Planner
import org.neo4j.cypher.internal.compatibility.v4_0.Cypher4_0Planner
import org.neo4j.cypher.internal.compatibility.{CypherPlanner, _}
import org.neo4j.cypher.internal.compiler.v4_0._
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.parallel._
import org.neo4j.cypher.internal.runtime.vectorized.Dispatcher
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}
import org.neo4j.scheduler.{Group, JobScheduler}

class EnterpriseCompilerFactory(community: CommunityCompilerFactory,
                                graph: GraphDatabaseQueryService,
                                kernelMonitors: KernelMonitors,
                                logProvider: LogProvider,
                                plannerConfig: CypherPlannerConfiguration,
                                runtimeConfig: CypherRuntimeConfiguration
                               ) extends CompilerFactory {
  /*
  One compiler is created for every Planner:Runtime:Version combination, e.g., Cost-Morsel-3.4 & Cost-Morsel-4.0.
  Each compiler contains a runtime instance, and each morsel runtime instance requires a dispatcher instance.
  This ensures only one (shared) dispatcher/tracer instance is created, even when there are multiple morsel runtime instances.
   */
  private val runtimeEnvironment = RuntimeEnvironment(runtimeConfig, graph.getDependencyResolver.resolveDependency(classOf[JobScheduler]))

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy): Compiler = {

    val log = logProvider.getLog(getClass)
    val planner = cypherVersion match {
      case CypherVersion.v3_4 =>
        Cypher3_4Planner(
          plannerConfig,
          MasterCompiler.CLOCK,
          kernelMonitors,
          log,
          cypherPlanner,
          cypherUpdateStrategy,
          LastCommittedTxIdProvider(graph))

      case CypherVersion.v4_0 =>
        Cypher4_0Planner(
          plannerConfig,
          MasterCompiler.CLOCK,
          kernelMonitors,
          log,
          cypherPlanner,
          cypherUpdateStrategy,
          LastCommittedTxIdProvider(graph))
    }

    CypherCurrentCompiler(
      planner,
      EnterpriseRuntimeFactory.getRuntime(cypherRuntime, plannerConfig.useErrorsOverWarnings),
      EnterpriseRuntimeContextCreator(GeneratedQueryStructure, log, plannerConfig, runtimeEnvironment),
      kernelMonitors)
  }
}

case class RuntimeEnvironment(config:CypherRuntimeConfiguration, jobScheduler: JobScheduler) {
  private val dispatcher: Dispatcher = createDispatcher()
  val tracer: SchedulerTracer = createTracer()

  def getDispatcher(debugOptions: Set[String]): Dispatcher =
    if (singleThreadedRequested(debugOptions) && !isAlreadySingleThreaded)
      new Dispatcher(config.morselSize, new SingleThreadScheduler())
    else
      dispatcher

  private def singleThreadedRequested(debugOptions: Set[String]) = debugOptions.contains("singlethreaded")

  private def isAlreadySingleThreaded = config.workers == 1

  private def createDispatcher(): Dispatcher = {
    val scheduler =
      if (config.workers == 1) new SingleThreadScheduler()
      else {
        val numberOfThreads = if (config.workers == 0) java.lang.Runtime.getRuntime.availableProcessors() else config.workers
        val executorService = jobScheduler.workStealingExecutor(Group.CYPHER_WORKER, numberOfThreads)
        new SimpleScheduler(executorService, config.waitTimeout)
      }
    new Dispatcher(config.morselSize, scheduler)
  }

  private def createTracer(): SchedulerTracer = {
    if (config.doSchedulerTracing)
      new DataPointSchedulerTracer(new ThreadSafeDataWriter(new CsvStdOutDataWriter))
    else
      SchedulerTracer.NoSchedulerTracer
  }
}

/**
  * Enterprise runtime context. Enriches the community runtime context with infrastructure needed for
  * query compilation and parallel execution.
  */
case class EnterpriseRuntimeContext(tokenContext: TokenContext,
                                    readOnly: Boolean,
                                    codeStructure: CodeStructure[GeneratedQuery],
                                    log: Log,
                                    clock: Clock,
                                    debugOptions: Set[String],
                                    config: CypherPlannerConfiguration,
                                    runtimeEnvironment: RuntimeEnvironment,
                                    compileExpressions: Boolean) extends RuntimeContext

/**
  * Creator of EnterpriseRuntimeContext
  */
case class EnterpriseRuntimeContextCreator(codeStructure: CodeStructure[GeneratedQuery],
                                           log: Log,
                                           config: CypherPlannerConfiguration,
                                           morselRuntimeState: RuntimeEnvironment)
  extends RuntimeContextCreator[EnterpriseRuntimeContext] {

  override def create(tokenContext: TokenContext,
                      clock: Clock,
                      debugOptions: Set[String],
                      readOnly: Boolean,
                      compileExpressions: Boolean): EnterpriseRuntimeContext =
    EnterpriseRuntimeContext(tokenContext,
                             readOnly,
                             codeStructure,
                             log,
                             clock,
                             debugOptions,
                             config,
                             morselRuntimeState,
                             compileExpressions)
}
