/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.runtime.morsel.tracing.{DataPointFlusher, SingleConsumerDataBuffers}

/**
  * Worker which polls scheduler tracer data and writes it to a [[DataPointFlusher]]. Makes sure to close
  * the flusher when interrupted.
  */
class SchedulerTracerOutputWorker(dataWriter: DataPointFlusher,
                                  dataBuffers: SingleConsumerDataBuffers) extends Runnable {

  override def run(): Unit =
    try {
      while (!Thread.interrupted()) {
        dataBuffers.consume(dataWriter)
        Thread.sleep(1)
      }
    } finally {
      dataWriter.close()
    }

}
