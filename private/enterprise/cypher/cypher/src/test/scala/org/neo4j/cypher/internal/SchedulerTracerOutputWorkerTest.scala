/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.mockito.Mockito.verify
import org.neo4j.cypher.internal.runtime.pipelined.tracing.DataPointFlusher
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SingleConsumerDataBuffers
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SchedulerTracerOutputWorkerTest extends CypherFunSuite {

  test("should close dataPointFlusher exiting") {
    val dataPointFlusher = mock[DataPointFlusher]
    val dataBuffers = new SingleConsumerDataBuffers()

    val tracerWorker = new SchedulerTracerOutputWorker(dataPointFlusher,
      dataBuffers,
      runnable => new Thread(runnable))
    tracerWorker.start()
    tracerWorker.stop()
    verify(dataPointFlusher).close()
  }
}
