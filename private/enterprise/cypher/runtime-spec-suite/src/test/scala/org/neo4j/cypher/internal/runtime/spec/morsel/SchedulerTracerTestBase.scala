/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import java.nio.file.{Files, Path}

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.morsel.SchedulerTracerTestBase._
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object SchedulerTracerTestBase {
  def newTempCSVPath(): Path = Files.createTempFile("scheduler-trace", ".csv")
  private val WORKER_COUNT: Int = Runtime.getRuntime.availableProcessors()
  private val MORSEL_SIZE: Int = 4
}


abstract class SchedulerTracerTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext], tempCSVPath: Path = SchedulerTracerTestBase.newTempCSVPath())
  extends RuntimeTestSuite[EnterpriseRuntimeContext](ENTERPRISE.PARALLEL.copyWith(
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> WORKER_COUNT.toString,
    GraphDatabaseSettings.enable_morsel_runtime_trace -> "true",
    GraphDatabaseSettings.morsel_scheduler_trace_filename -> tempCSVPath.toAbsolutePath.toString
  ), runtime) {

  override def afterShutdown(): Unit = {
    Files.delete(tempCSVPath)
  }

  test("should trace big expand query correctly") {

    // GIVEN
    val SIZE = 10

    bipartiteGraph(SIZE,"A","B","R")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n1", "n3")
      .expand("(n2)--(n3)")
      .expand("(n1)--(n2)")
      .allNodeScan("n1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedRowCount = 2000
    runtimeResult should beColumns("n1", "n3").withRows(rowCount(expectedRowCount))

    Thread.sleep(1000) // allow tracer output daemon to finish

    val (header, dataRows) = parseTrace(tempCSVPath.toAbsolutePath)
    header should be (Array("id_1.0",
                             "upstreamIds",
                             "queryId",
                             "schedulingThreadId",
                             "schedulingTime(us)",
                             "executionThreadId",
                             "startTime(us)",
                             "stopTime(us)",
                             "pipelineId",
                             "pipelineDescription"))

    dataRows.size should be >= expectedRowCount / MORSEL_SIZE

    val queryIds = mutable.Set[Long]()
    val dataLookup = mutable.Map[Long, DataRow]()
    val executionThreadIds = mutable.Set[Long]()
    val schedulingThreadIds = mutable.Set[Long]()

    for (dataRow <- dataRows) {
      dataRow.schedulingTime should be <= dataRow.startTime
      dataRow.startTime should be <= dataRow.stopTime
      dataRow.pipelineId should be >= 0L
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
    withClue("Expect no duplicate work unit IDs"){
      dataLookup.size should be(dataRows.size)
    }

    for (dataRow <- dataRows) {
      for (upstreamId <- dataRow.upstreamIds) {
        dataLookup.get(upstreamId) match {
          case None =>
            fail(s"Could not find upstream data row with id $upstreamId")
          case Some(upstream) =>
            upstream.stopTime should be < dataRow.startTime
        }
      }
    }
  }

  private def parseTrace(path: Path): (Array[String], ArrayBuffer[DataRow]) = {

    var header: Array[String] = null
    val dataRows: ArrayBuffer[DataRow] = new ArrayBuffer

    val source = Source.fromFile(path.toFile)
    try {
      for (line <- source.getLines()) {
        val parts = line.split(",").map(_.trim)
        if (header == null)
          header = parts
        else
          dataRows += DataRow(
            parts(0).toLong,
            parseUpstreams(parts(1)),
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
    } finally  {
      source.close()
    }
  }

  private def parseUpstreams(upstreams: String): Seq[Long] = {
    upstreams.slice(1, upstreams.length - 1).split(',').filter(_.nonEmpty).map(_.toLong)
  }

  private case class DataRow(id: Long,
                             upstreamIds: Seq[Long],
                             queryId: Long,
                             schedulingThreadId: Long,
                             schedulingTime: Long,
                             executionThreadId: Long,
                             startTime: Long,
                             stopTime: Long,
                             pipelineId: Long,
                             pipelineDescription: String)
}
