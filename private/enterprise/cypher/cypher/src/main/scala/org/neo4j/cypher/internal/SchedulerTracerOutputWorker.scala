/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.runtime.pipelined.tracing.DataPointFlusher
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SingleConsumerDataBuffers
import org.neo4j.kernel.lifecycle.LifecycleAdapter

import scala.concurrent.duration.Duration

/**
 * Worker which polls scheduler tracer data and writes it to a [[DataPointFlusher]]. Makes sure to close
 * the flusher when interrupted.
 */
class SchedulerTracerOutputWorker(dataWriter: DataPointFlusher,
                                  dataBuffers: SingleConsumerDataBuffers,
                                  threadFactory: ThreadFactory) extends LifecycleAdapter {

  private val threadJoinWait = Duration(1, TimeUnit.MINUTES)

  @volatile
  private var isTimeToStop = false

  @volatile
  private var thread: Thread = _

  private def run(): Unit =
    try {
      while (!isTimeToStop) {
        dataBuffers.consume(dataWriter)
        Thread.sleep(1)
      }
    } finally {
      dataBuffers.consume(dataWriter)
      dataWriter.close()
    }

  override def start(): Unit = {
    isTimeToStop = false
    thread = threadFactory.newThread(() => run())
    thread.start()
  }

  override def stop(): Unit = {
    isTimeToStop = true
    thread.join(threadJoinWait.toMillis)
  }
}
