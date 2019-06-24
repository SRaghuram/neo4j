/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.runtime.morsel.tracing.{DataPointFlusher, SingleConsumerDataBuffers}
import org.neo4j.kernel.lifecycle.Lifecycle

/**
  * Worker which polls scheduler tracer data and writes it to a [[DataPointFlusher]]. Makes sure to close
  * the flusher when interrupted.
  */
class SchedulerTracerOutputWorker(dataWriter: DataPointFlusher,
                                  dataBuffers: SingleConsumerDataBuffers) extends Runnable with Lifecycle {

  @volatile
  private var isTimeToStop = false

  override def run(): Unit =
    try {
      while (!isTimeToStop) {
        dataBuffers.consume(dataWriter)
        Thread.sleep(1)
      }
    } catch {
      case e: InterruptedException =>
        // expected
    } finally {
      dataBuffers.consume(dataWriter)
      dataWriter.close()
    }

  override def init(): Unit = {}

  override def start(): Unit = {
    isTimeToStop = false
  }

  override def stop(): Unit = {
    isTimeToStop = true
  }

  override def shutdown(): Unit = {}
}
