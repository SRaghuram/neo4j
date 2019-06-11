/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherMorselRuntimeSchedulerOption._
import org.neo4j.cypher.internal.runtime.morsel.execution.{CallingThreadQueryExecutor, FixedWorkersQueryExecutor, NO_TRANSACTION_BINDER, QueryExecutor, QueryResources}
import org.neo4j.cypher.internal.runtime.morsel.tracing.{CsvFileDataWriter, CsvStdOutDataWriter, DataPointSchedulerTracer, SchedulerTracer, SingleConsumerDataBuffers}
import org.neo4j.cypher.internal.v4_0.util.InternalException
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
         lifeSupport: LifeSupport): RuntimeEnvironment = {

    new RuntimeEnvironment(config,
                           createQueryExecutor(config, jobScheduler, cursors, txBridge, lifeSupport),
                           createTracer(config, jobScheduler),
                           cursors)
  }

  def createQueryExecutor(config: CypherRuntimeConfiguration,
                          jobScheduler: JobScheduler,
                          cursors: CursorFactory,
                          txBridge: ThreadToStatementContextBridge,
                          lifeSupport: LifeSupport): QueryExecutor =
    config.scheduler match {
      case SingleThreaded =>
        new CallingThreadQueryExecutor(config.morselSize, NO_TRANSACTION_BINDER, cursors)
      case Simple | LockFree =>
        val threadFactory = jobScheduler.interruptableThreadFactory(Group.CYPHER_WORKER)
        val numberOfThreads = if (config.workers == 0) java.lang.Runtime.getRuntime.availableProcessors() else config.workers
        val txBinder = new TxBridgeTransactionBinder(txBridge)
        val queryExecutor = new FixedWorkersQueryExecutor(config.morselSize, threadFactory, numberOfThreads, txBinder, () => new QueryResources(cursors))
        lifeSupport.add(queryExecutor)
        queryExecutor
    }

  def createTracer(config: CypherRuntimeConfiguration, jobScheduler: JobScheduler): SchedulerTracer = {
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

      val runnable = new SchedulerTracerOutputWorker(dataWriter, dataTracer)
      jobScheduler.interruptableThreadFactory(Group.CYPHER_WORKER).newThread(runnable).start()

      new DataPointSchedulerTracer(dataTracer)
    }
  }
}

class RuntimeEnvironment(config: CypherRuntimeConfiguration,
                         queryExecutor: QueryExecutor,
                         val tracer: SchedulerTracer,
                         val cursors: CursorFactory) {

  def getQueryExecutor(parallelExecution: Boolean, debugOptions: Set[String]): QueryExecutor =
    if ((!parallelExecution || MorselOptions.singleThreaded(debugOptions)) && !isAlreadySingleThreaded)
      new CallingThreadQueryExecutor(config.morselSize, NO_TRANSACTION_BINDER, cursors)
    else
      queryExecutor

  private def isAlreadySingleThreaded = config.scheduler == SingleThreaded
}
