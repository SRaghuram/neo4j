/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerCache
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.execution.CallingThreadQueryExecutor
import org.neo4j.cypher.internal.runtime.pipelined.execution.FixedWorkersQueryExecutor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryExecutor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.tracing.CsvDataWriter
import org.neo4j.cypher.internal.runtime.pipelined.tracing.CsvFileDataWriter
import org.neo4j.cypher.internal.runtime.pipelined.tracing.CsvStdOutDataWriter
import org.neo4j.cypher.internal.runtime.pipelined.tracing.DataPointSchedulerTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SingleConsumerDataBuffers
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobMonitoringParams.systemJob
import org.neo4j.scheduler.JobScheduler

/**
 * System environment in which runtimes operate. Includes access to global (per database?) resources
 * like jobScheduler and config.
 */
object RuntimeEnvironment {
  def of(config: CypherRuntimeConfiguration,
         jobScheduler: JobScheduler,
         cursors: CursorFactory,
         lifeSupport: LifeSupport,
         workerManager: WorkerManagement,
         memoryTracker: MemoryTracker): RuntimeEnvironment = {

    new RuntimeEnvironment(
      createPipelinedQueryExecutor(cursors),
      createParallelQueryExecutor(cursors, lifeSupport, workerManager, memoryTracker),
      createTracer(config, jobScheduler, lifeSupport),
      cursors,
      createCache(jobScheduler))
  }

  private def createPipelinedQueryExecutor(cursors: CursorFactory) = {
    new CallingThreadQueryExecutor(cursors)
  }

  private def createCache(jobScheduler: JobScheduler) = {
    val monitoredExecutor = jobScheduler.monitoredJobExecutor(Group.CYPHER_CACHE)
    val cacheFactory = new ExecutorBasedCaffeineCacheFactory((job: Runnable) => monitoredExecutor.execute(systemJob("Compiled expressions cache maintenance"), job));
    new CachingExpressionCompilerCache(cacheFactory)
  }

  private def createParallelQueryExecutor(cursors: CursorFactory,
                                          lifeSupport: LifeSupport,
                                          workerManager: WorkerManagement,
                                          memoryTracker: MemoryTracker): QueryExecutor = {
    val resourceFactory = (workerId: Int) => new QueryResources(cursors, PageCursorTracer.NULL, memoryTracker, workerId, workerManager.numberOfWorkers)
    val workerResourceProvider = new WorkerResourceProvider(workerManager.numberOfWorkers, resourceFactory)
    lifeSupport.add(workerResourceProvider)
    val queryExecutor = new FixedWorkersQueryExecutor(workerResourceProvider, workerManager)
    queryExecutor
  }

  def createTracer(config: CypherRuntimeConfiguration,
                   jobScheduler: JobScheduler,
                   lifeSupport: LifeSupport): SchedulerTracer = {
    config.schedulerTracing match {
      case NoSchedulerTracing => SchedulerTracer.NoSchedulerTracer
      case StdOutSchedulerTracing => dataPointTracer(jobScheduler, lifeSupport, new CsvStdOutDataWriter)
      case FileSchedulerTracing(file) => dataPointTracer(jobScheduler, lifeSupport, new CsvFileDataWriter(file))
    }
  }

  private def dataPointTracer(jobScheduler: JobScheduler,
                              lifeSupport: LifeSupport,
                              dataWriter: CsvDataWriter) = {
    val dataTracer = new SingleConsumerDataBuffers()
    val threadFactory = jobScheduler.threadFactory(Group.CYPHER_WORKER)
    val tracerWorker = new SchedulerTracerOutputWorker(dataWriter, dataTracer, threadFactory)
    lifeSupport.add(tracerWorker)

    new DataPointSchedulerTracer(dataTracer)
  }
}

class RuntimeEnvironment(pipelinedQueryExecutor: QueryExecutor,
                         parallelQueryExecutor: QueryExecutor,
                         val tracer: SchedulerTracer,
                         val cursors: CursorFactory,
                         expressionCache: CachingExpressionCompilerCache) {

  def getQueryExecutor(parallelExecution: Boolean): QueryExecutor =
    if (parallelExecution) parallelQueryExecutor else pipelinedQueryExecutor


  def getCompiledExpressionCache: CachingExpressionCompilerCache = expressionCache

  /**
   * Create a copy of this runtime environment but switch the scheduler tracer implementation.
   */
  def withSchedulerTracer(tracer: SchedulerTracer): RuntimeEnvironment = {
    new RuntimeEnvironment(
      pipelinedQueryExecutor,
      parallelQueryExecutor,
      tracer,
      cursors,
      expressionCache)
  }
}

