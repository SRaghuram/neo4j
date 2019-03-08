/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.scheduling

case class DataPoint(id: Long,
                     upstreamIds: Seq[Long],
                     queryId: Int,
                     schedulingThreadId: Long,
                     scheduledTime: Long,
                     executionThreadId: Long,
                     startTime: Long,
                     stopTime: Long,
                     workId: WorkIdentity) {

  def withTimeZero(t0: Long): DataPoint =
    DataPoint(
      id,
      upstreamIds,
      queryId,
      schedulingThreadId,
      scheduledTime - t0,
      executionThreadId,
      startTime - t0,
      stopTime - t0,
      workId)
}

/**
  * Write data points to somewhere.
  */
trait DataPointWriter {

  /**
    * Write (e.g., log) this tracing data point
    */
  def write(dataPoint: DataPoint): Unit
}

/**
  * Write data points to somewhere.
  */
trait DataPointFlusher extends DataPointWriter {

  /**
    * Flush buffered data points
    */
  def flush(): Unit
}

