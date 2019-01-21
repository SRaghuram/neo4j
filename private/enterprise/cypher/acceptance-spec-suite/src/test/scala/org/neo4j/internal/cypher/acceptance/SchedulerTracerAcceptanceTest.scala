/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.nio.file.Files

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.io.File

class SchedulerTracerAcceptanceTest extends ExecutionEngineFunSuite {

  val MORSEL_SIZE = 4 // The morsel size to use in the config for testing
  val WORKER_COUNT = 5 // Number of executing threads

  private val path = Files.createTempFile("scheduler-trace", "csv")
  private val file = new File(path.toFile)

  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> WORKER_COUNT.toString,
    GraphDatabaseSettings.enable_morsel_runtime_trace -> "true",
    GraphDatabaseSettings.morsel_scheduler_trace_filename -> file.toString()
  )

  override protected def stopTest(): Unit = {
    try {
      file.delete()
    } finally {
      super.stopTest()
    }
  }

  test("should trace big expand query correctly") {

    // GIVEN
    val SIZE = 10
    val ns = new ArrayBuffer[Node]()
    val ms = new ArrayBuffer[Node]()

    graph.inTx {
      for (i <- 1 to SIZE) ns += createLabeledNode("N")
      for (i <- 1 to SIZE) ms += createLabeledNode("M")
    }

    graph.inTx {
      for {n <- ns; m <- ms} {
        relate(n, m, "R")
      }
    }

    // WHEN
    val result = graph.execute("CYPHER runtime=morsel MATCH (n1:N)--(m1)--(n2)--(m2) RETURN id(m2)")

    var count = 0L
    while (result.hasNext) {
      result.next()
      count += 1
    }

    // THEN
    count should be(8100)

    Thread.sleep(1000) // allow tracer output daemon to finish

    val (header, dataRows) = parseTrace(file.toString())
    header should be (Array("id",
                             "upstreamId",
                             "queryId",
                             "schedulingThreadId",
                             "schedulingTime(us)",
                             "executionThreadId",
                             "startTime(us)",
                             "stopTime(us)",
                             "pipelineId",
                             "pipelineDescription"))

    val queryIds = mutable.Set[Long]()
    val dataLookup = mutable.Map[Long, DataRow]()
    val executionThreadIds = mutable.Set[Long]()
    val schedulingThreadIds = mutable.Set[Long]()

    for (dataRow <- dataRows) {
      dataRow.schedulingTime should be <= dataRow.startTime
      dataRow.startTime should be <= dataRow.stopTime
      dataRow.pipelineDescription should not be ""

      queryIds += dataRow.queryId
      dataLookup += dataRow.id -> dataRow
      schedulingThreadIds += dataRow.schedulingThreadId
      executionThreadIds += dataRow.executionThreadId
    }

    queryIds.size should be(1)
    schedulingThreadIds.size should be <= (WORKER_COUNT + 1)
    executionThreadIds.size should be <= WORKER_COUNT
    executionThreadIds.size should be > 1
    dataLookup.size should be(dataRows.size)

    for (dataRow <- dataRows) {
      val upstreamId = dataRow.upstreamId
      if (upstreamId != -1) {
        dataLookup.get(upstreamId) match {
          case None =>
            fail(s"Could not find upstream data row with id $upstreamId")
          case Some(upstream) =>
            upstream.stopTime should be < dataRow.startTime

        }
      }
    }
  }

  private def parseTrace(path: String): (Array[String], ArrayBuffer[DataRow]) = {

    var header: Array[String] = null
    val dataRows: ArrayBuffer[DataRow] = new ArrayBuffer

    for (line <- Source.fromFile(path).getLines()) {
      val parts = line.split(",").map(_.trim)
      if (header == null)
        header = parts
      else
        dataRows += DataRow(
          parts(0).toLong,
          parts(1).toLong,
          parts(2).toInt,
          parts(3).toLong,
          parts(4).toLong,
          parts(5).toLong,
          parts(6).toLong,
          parts(7).toLong,
          parts(8).toLong,
          parts(9)
        )
    }

    (header, dataRows)
  }

  private case class DataRow(id: Long,
                             upstreamId: Long,
                             queryId: Long,
                             schedulingThreadId: Long,
                             schedulingTime: Long,
                             executionThreadId: Long,
                             startTime: Long,
                             stopTime: Long,
                             pipelineId: Long,
                             pipelineDescription: String)
}
