/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.time.Clock

import org.neo4j.cypher.CypherMorselRuntimeSchedulerOption._
import org.neo4j.cypher.CypherPlannerOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_5.Cypher3_5Planner
import org.neo4j.cypher.internal.compatibility.v4_0.Cypher4_0Planner
import org.neo4j.cypher.internal.compiler.v4_0._
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.parallel._
import org.neo4j.cypher.internal.runtime.vectorized.Dispatcher
import org.neo4j.cypher.internal.runtime.vectorized.NO_TRANSACTION_BINDER
import org.neo4j.cypher.internal.runtime.vectorized.QueryResources
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.Kernel
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobScheduler

class EnterpriseCompilerFactory(community: CommunityCompilerFactory,
                                graph: GraphDatabaseQueryService,
                                kernelMonitors: KernelMonitors,
                                logProvider: LogProvider,
                                plannerConfig: CypherPlannerConfiguration,
                                runtimeConfig: CypherRuntimeConfiguration
                               ) extends CompilerFactory {
  /*
  One compiler is created for every Planner:Runtime:Version combination, e.g., Cost-Morsel-3.5 & Cost-Morsel-4.0.
  Each compiler contains a runtime instance, and each morsel runtime instance requires a dispatcher instance.
  This ensures only one (shared) dispatcher/tracer instance is created, even when there are multiple morsel runtime instances.
   */
  private val runtimeEnvironment = {
    val resolver = graph.getDependencyResolver
    val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
    val kernel = resolver.resolveDependency(classOf[Kernel])
    val txBridge = resolver.resolveDependency(classOf[ThreadToStatementContextBridge])
    RuntimeEnvironment(runtimeConfig, jobScheduler, kernel.cursors(), txBridge)
  }

  override def createCompiler(cypherVersion: CypherVersion,
                              cypherPlanner: CypherPlannerOption,
                              cypherRuntime: CypherRuntimeOption,
                              cypherUpdateStrategy: CypherUpdateStrategy): Compiler = {

    val log = logProvider.getLog(getClass)
    val planner = cypherVersion match {
      case CypherVersion.`v3_5` =>
        Cypher3_5Planner(
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

case class RuntimeEnvironment(config:CypherRuntimeConfiguration,
                              jobScheduler: JobScheduler,
                              cursors: CursorFactory,
                              txBridge: ThreadToStatementContextBridge) {

  private val dispatcher: Dispatcher = createDispatcher()
  val tracer: SchedulerTracer = createTracer()

  def getDispatcher(debugOptions: Set[String]): Dispatcher =
    if (singleThreadedRequested(debugOptions) && !isAlreadySingleThreaded)
      new Dispatcher(config.morselSize, new SingleThreadScheduler(() => new QueryResources(cursors)), NO_TRANSACTION_BINDER)
    else
      dispatcher

  private def singleThreadedRequested(debugOptions: Set[String]) = debugOptions.contains("singlethreaded")

  private def isAlreadySingleThreaded = config.scheduler == SingleThreaded

  private def createDispatcher(): Dispatcher = {
    val (scheduler, transactionBinder) = config.scheduler match {
      case SingleThreaded =>
        (new SingleThreadScheduler(() => new QueryResources(cursors)), NO_TRANSACTION_BINDER)
      case Simple =>
        val numberOfThreads = if (config.workers == 0) java.lang.Runtime.getRuntime.availableProcessors() else config.workers
        val executorService = jobScheduler.workStealingExecutor(Group.CYPHER_WORKER, numberOfThreads)
        (new SimpleScheduler(executorService, config.waitTimeout, () => new QueryResources(cursors), numberOfThreads), new TxBridgeTransactionBinder(txBridge))
      case LockFree =>
        val numberOfThreads = if (config.workers == 0) java.lang.Runtime.getRuntime.availableProcessors() else config.workers
        val threadFactory = jobScheduler.interruptableThreadFactory(Group.CYPHER_WORKER)
        (new LockFreeScheduler(threadFactory, numberOfThreads, config.waitTimeout, () => new QueryResources(cursors)), new TxBridgeTransactionBinder(txBridge))
    }
    new Dispatcher(config.morselSize, scheduler, transactionBinder)
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
                                    schemaRead: SchemaRead,
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
                      schemaRead: SchemaRead,
                      clock: Clock,
                      debugOptions: Set[String],
                      readOnly: Boolean,
                      compileExpressions: Boolean): EnterpriseRuntimeContext =
    EnterpriseRuntimeContext(tokenContext,
                             schemaRead,
                             readOnly,
                             codeStructure,
                             log,
                             clock,
                             debugOptions,
                             config,
                             morselRuntimeState,
                             compileExpressions)
}
