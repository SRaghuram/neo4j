/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.runtime.pipelined.tracing.CsvDataWriter.HEADER
import org.neo4j.cypher.internal.runtime.pipelined.tracing.CsvDataWriter.SEPARATOR

/**
 * DataPointWriter which accepts DataPoints and formats as CSV.
 */
abstract class CsvDataWriter extends DataPointFlusher {


  override final def write(dp: DataPoint): Unit = writeRow(serialize(dp))

  def writeRow(row: String): Unit

  protected def header: String = HEADER

  private def serialize(dataPoint: DataPoint): String =
    Array(
      dataPoint.id.toString,
      dataPoint.upstreamId.mkString("[", ";", "]"),
      dataPoint.queryId.toString,
      dataPoint.schedulingThreadId.toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.scheduledTime).toString,
      dataPoint.executionThreadId.toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.startTime).toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.stopTime).toString,
      dataPoint.workId.workId.x,
      dataPoint.workId.workDescription
    ).mkString(SEPARATOR) + System.lineSeparator()
}

object CsvDataWriter {
  private val SEPARATOR = ","
  private val VERSION = "1.0"
  private val HEADER = Array(s"id_$VERSION",
    "upstreamIds",
    "queryId",
    "schedulingThreadId",
    "schedulingTime(us)",
    "executionThreadId",
    "startTime(us)",
    "stopTime(us)",
    "pipelineId",
    "pipelineDescription").mkString(SEPARATOR) + System.lineSeparator()
}
