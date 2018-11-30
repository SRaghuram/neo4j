/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.TimeUnit

/**
  * DataPointWriter which accepts DataPoints, formats as CSV, and prints to std out.
  */
class CsvStdOutDataWriter extends DataPointWriter {

  import CsvStdOutDataWriter._

  private val sb = new StringBuilder(HEADER)

  def flush(): Unit = {
    val result = sb.result()
    sb.clear()
    sb ++= HEADER
    println(result)
  }

  override def write(dp: DataPoint): Unit =
    sb ++= serialize(dp)

  private def serialize(dataPoint: DataPoint): String =
    Array(
      dataPoint.id.toString,
      dataPoint.upstreamId.toString,
      dataPoint.queryId.toString,
      dataPoint.schedulingThreadId.toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.scheduledTime).toString,
      dataPoint.executionThreadId.toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.startTime).toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.stopTime).toString,
      dataPoint.task.workId,
      dataPoint.task.workDescription
    ).mkString(SEPARATOR) + System.lineSeparator()
}

object CsvStdOutDataWriter {
  private val SEPARATOR = ","
  private val HEADER = Array("id",
                             "upstreamId",
                             "queryId",
                             "schedulingThreadId",
                             "schedulingTime(us)",
                             "executionThreadId",
                             "startTime(us)",
                             "stopTime(us)",
                             "pipelineId",
                             "pipelineDescription").mkString(SEPARATOR) + System.lineSeparator()
}
