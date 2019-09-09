/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.runtime.morsel.tracing._
import org.neo4j.cypher.internal.runtime.morsel.{WorkerManagement, WorkerResourceProvider}
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.scheduler.{Group, JobScheduler}

/**
  * System environment in which runtimes operate. Includes access to global (per database?) resources
  * like jobScheduler and config.
  */
object RuntimeEnvironment {
  def of(config: CypherRuntimeConfiguration,
         jobScheduler: JobScheduler,
         cursors: CursorFactory,
         txBridge: ThreadToStatementContextBridge,
         lifeSupport: LifeSupport,
         workerManager: WorkerManagement): RuntimeEnvironment = {

    new RuntimeEnvironment(config,
      createMorselQueryExecutor(cursors),
      createParallelQueryExecutor(cursors, txBridge, lifeSupport, workerManager),
      createTracer(config, jobScheduler, lifeSupport),
      cursors)
  }

  private def createMorselQueryExecutor(cursors: CursorFactory) = {
    new CallingThreadQueryExecutor(NO_TRANSACTION_BINDER, cursors)
  }

  private def createParallelQueryExecutor(cursors: CursorFactory,
                                  txBridge: ThreadToStatementContextBridge,
                                  lifeSupport: LifeSupport,
                                  workerManager: WorkerManagement): QueryExecutor = {
    val txBinder = new TxBridgeTransactionBinder(txBridge)
    val resourceFactory = () => new QueryResources(cursors)
    val workerResourceProvider = new WorkerResourceProvider(workerManager.numberOfWorkers, resourceFactory)
    lifeSupport.add(workerResourceProvider)
    val queryExecutor = new FixedWorkersQueryExecutor(txBinder, workerResourceProvider, workerManager)
    queryExecutor
  }

  def createTracer(config: CypherRuntimeConfiguration,
                   jobScheduler: JobScheduler,
                   lifeSupport: LifeSupport): SchedulerTracer = {
    if (config.schedulerTracing == NoSchedulerTracing)
      SchedulerTracer.NoSchedulerTracer
    else {
      val dataWriter =
        config.schedulerTracing match {
          case StdOutSchedulerTracing => new CsvStdOutDataWriter
          case FileSchedulerTracing(file) => new CsvFileDataWriter(file)
          case tracing => throw new InternalException(s"Unknown scheduler tracing: $tracing")
        }

      val dataTracer = new SingleConsumerDataBuffers()
      val threadFactory = jobScheduler.threadFactory(Group.CYPHER_WORKER)
      val tracerWorker = new SchedulerTracerOutputWorker(dataWriter, dataTracer, threadFactory)
      lifeSupport.add(tracerWorker)

      new DataPointSchedulerTracer(dataTracer)
    }
  }
}

class RuntimeEnvironment(config: CypherRuntimeConfiguration,
                         morselQueryExecutor: QueryExecutor,
                         parallelQueryExecutor: QueryExecutor,
                         val tracer: SchedulerTracer,
                         val cursors: CursorFactory) {

  def getQueryExecutor(parallelExecution: Boolean): QueryExecutor =
    if (parallelExecution) parallelQueryExecutor else morselQueryExecutor

}
