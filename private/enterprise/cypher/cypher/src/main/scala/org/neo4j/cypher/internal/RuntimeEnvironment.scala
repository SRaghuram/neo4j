/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherMorselRuntimeSchedulerOption._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.runtime.scheduling._
import org.neo4j.cypher.internal.runtime.morsel.{Dispatcher, NO_TRANSACTION_BINDER, QueryResources}
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.scheduler.{Group, JobScheduler}

/**
  * System environment in which runtimes operate. Includes access to global (per database?) resources
  * like jobScheduler and config.
  */
object RuntimeEnvironment {
  def of(config: CypherRuntimeConfiguration,
         jobScheduler: JobScheduler,
         cursors: CursorFactory,
         txBridge: ThreadToStatementContextBridge): RuntimeEnvironment = {

    new RuntimeEnvironment(config,
                           createDispatcher(config, jobScheduler, cursors, txBridge),
                           createTracer(config, jobScheduler),
                           cursors)
  }

  def createDispatcher(config: CypherRuntimeConfiguration,
                       jobScheduler: JobScheduler,
                       cursors: CursorFactory,
                       txBridge: ThreadToStatementContextBridge): Dispatcher = {
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

  def createTracer(config: CypherRuntimeConfiguration, jobScheduler: JobScheduler): SchedulerTracer = {
    if (config.schedulerTracing == NoSchedulerTracing)
      SchedulerTracer.NoSchedulerTracer
    else {
      val dataWriter =
        config.schedulerTracing match {
          case StdOutSchedulerTracing => new CsvStdOutDataWriter
          case FileSchedulerTracing(file) => new CsvFileDataWriter(file)
        }

      val dataTracer = new SingleConsumerDataBuffers()

      val runnable = new Runnable {
        override def run(): Unit =
          while (!Thread.interrupted()) {
            dataTracer.consume(dataWriter)
            Thread.sleep(1)
          }
      }
      jobScheduler.threadFactory(Group.CYPHER_WORKER).newThread(runnable).start()

      new DataPointSchedulerTracer(dataTracer)
    }
  }
}

class RuntimeEnvironment(config: CypherRuntimeConfiguration,
                         dispatcher: Dispatcher,
                         val tracer: SchedulerTracer,
                         val cursors: CursorFactory) {

  def getDispatcher(debugOptions: Set[String]): Dispatcher =
    if (MorselOptions.singleThreaded(debugOptions) && !isAlreadySingleThreaded)
      new Dispatcher(config.morselSize, new SingleThreadScheduler(() => new QueryResources(cursors)), NO_TRANSACTION_BINDER)
    else
      dispatcher

  private def isAlreadySingleThreaded = config.scheduler == SingleThreaded
}
