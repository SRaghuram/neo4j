/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

case class DataPoint(id: Long,
                     upstreamId: Long,
                     queryId: Int,
                     schedulingThreadId: Long,
                     scheduledTime: Long,
                     executionThreadId: Long,
                     startTime: Long,
                     stopTime: Long,
                     task: Task[_ <: AutoCloseable]) {

  def withTimeZero(t0: Long): DataPoint =
    DataPoint(id,
      upstreamId,
      queryId,
      schedulingThreadId,
      scheduledTime - t0,
      executionThreadId,
      startTime - t0,
      stopTime - t0,
      task)
}

/**
  * Write data points to somewhere.
  */
trait DataPointWriter {

  /**
    * Write (e.g., log) this tracing data point
    */
  def write(dataPoint: DataPoint): Unit

  /**
    * Flush buffered data points
    */
  def flush(): Unit
}

