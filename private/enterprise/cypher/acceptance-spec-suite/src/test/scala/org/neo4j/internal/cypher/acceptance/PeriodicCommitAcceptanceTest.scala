/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter

import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.TxCounts
import org.neo4j.cypher.TxCountsTrackingTestSupport
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.exceptions
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.PeriodicCommitInOpenTransactionException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.storageengine.api.TransactionIdStore

class PeriodicCommitAcceptanceTest extends ExecutionEngineFunSuite
  with TxCountsTrackingTestSupport
  with QueryStatisticsTestSupport
  with CreateTempFileTestSupport
  with ResourceTracking {

  override protected def initTest(): Unit = {
    super.initTest()
    trackResources(graph)
  }

  def unwrapLoadCSVStatus[T](f: => T) = {
    try {
      f
    }
    catch {
      case t: Throwable => throw ExceptionUtils.getRootCause(t)
    }
  }

  def createTempCSVFile(numberOfLines: Int): String =
    createTempFileURL("file", ".csv") { writer: PrintWriter =>
      1.to(numberOfLines).foreach { n: Int => writer.println(n.toString) }
    }

  private def createFile(f: PrintWriter => Unit) = createTempFileURL("cypher", ".csv")(f).cypherEscape

  test("should reject periodic commit when not followed by LOAD CSV") {
    intercept[SyntaxException] {
      executeScalar("USING PERIODIC COMMIT 200 MATCH (n) RETURN count(n)")
    }
  }

  test("should produce data from periodic commit") {
    val url = createTempFileURL("foo", ".csv") { writer: PrintWriter =>
      writer.println("42")
    }
    val result = execute(s"USING PERIODIC COMMIT 200 LOAD CSV FROM '$url' AS line CREATE (n {id: line[0]}) RETURN n.id")

    result.toList should equal(List(Map("n.id" -> "42")))
    result.columns should equal(List("n.id"))
    resourceMonitor.assertClosedAndClear(1)
  }

  test("should use cost planner for periodic commit and load csv") {
    val url = createTempFileURL("foo", ".csv") { writer: PrintWriter =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
      writer.println("5")
    }

    // to make sure the property key id is created before the tx in order to not mess up with the tx counts
    createNode(Map("id" -> 42))

    val txIdStore = graph.getDependencyResolver.resolveDependency(classOf[TransactionIdStore])
    val beforeTxId = txIdStore.getLastClosedTransactionId
    val result = execute(s"PROFILE USING PERIODIC COMMIT 1 LOAD CSV FROM '$url' AS line CREATE (n {id: line[0]}) RETURN n.id as id")
    val arguments = result.executionPlanDescription().arguments
    arguments should contain(PlannerImpl("IDP"))
    arguments.find( _.isInstanceOf[PageCacheHits]) shouldBe defined
    arguments.find( _.isInstanceOf[PageCacheMisses]) shouldBe defined
    result.columnAs[Long]("id").toList should equal(List("1","2","3","4","5"))
    resourceMonitor.assertClosedAndClear(1)

    val afterTxId = txIdStore.getLastClosedTransactionId
    afterTxId should equal(beforeTxId + 5)
  }

  test("should support simple periodic commit") {
    // given
    val url = createTempCSVFile(5)
    val queryText =
      "USING PERIODIC COMMIT 2 " +
        s"LOAD CSV FROM '$url' AS line " +
        "CREATE ()"

    // when
    val (result, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    assertStats(result, nodesCreated = 5)
    resourceMonitor.assertClosedAndClear(1)

    // and then
    txCounts should equal(TxCounts(commits = 3))
  }

  test("should support simple periodic commit with unaligned batch size") {
    // given
    val url = createTempCSVFile(4)
    val queryText =
      "USING PERIODIC COMMIT 3 " +
        s"LOAD CSV FROM '$url' AS line " +
        "CREATE ()"

    // when
    val (result, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    assertStats(result, nodesCreated = 4)
    resourceMonitor.assertClosedAndClear(1)

    // and then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("should abort first tx when failing on first batch during periodic commit") {
    // given
    val url = createTempCSVFile(20)
    val query = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE ({x: (toInteger(line[0]) - 8)/0})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[exceptions.ArithmeticException](
      unwrapLoadCSVStatus(executeScalar[Number](query))
    ))

    // then
    resourceMonitor.assertClosedAndClear(1)
    txCounts should equal(TxCounts(rollbacks = 1))
  }

  test("should not mistakenly use closed statements") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line MERGE (:Label);"

    // when
    val (_, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    resourceMonitor.assertClosedAndClear(1)
    txCounts should equal(TxCounts(commits = 2, rollbacks = 0))
  }

  test("should commit first tx and abort second tx when failing on second batch during periodic commit") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE ({x: 1 / (toInteger(line[0]) - 16)})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[exceptions.ArithmeticException](
      unwrapLoadCSVStatus(graph.getGraphDatabaseService.executeTransactionally(queryText))
    ))

    // then
    resourceMonitor.assertClosedAndClear(1)
    txCounts should equal(TxCounts(commits = 1, rollbacks = 1))
  }

  test("should support periodic commit hint without explicit size") {
    val url = createTempCSVFile(1)
    executeScalar[Node](s"USING PERIODIC COMMIT LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
    resourceMonitor.assertClosedAndClear(1)
  }

  test("should support periodic commit hint with explicit size") {
    val url = createTempCSVFile(1)
    executeScalar[Node](s"USING PERIODIC COMMIT 400 LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
    resourceMonitor.assertClosedAndClear(1)
  }

  test("should reject periodic commit hint with negative size") {
    val url = createTempCSVFile(1)
    intercept[SyntaxException] {
      executeScalar[Node](s"USING PERIODIC COMMIT -1 LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
    }
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should fail if periodic commit is executed in an open transaction") {
    // given
    val transaction = graph.getGraphDatabaseService.beginTx()
    try {
      val caught = intercept[QueryExecutionException] {
        val url = createTempCSVFile(3)
        transaction.execute(s"USING PERIODIC COMMIT LOAD CSV FROM '$url' AS line CREATE ()")
      }
      val rootCause = ExceptionUtils.getRootCause(caught)
      rootCause shouldBe a[PeriodicCommitInOpenTransactionException]
    }
    finally {
      transaction.close()
    }
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should tell line number information when failing using periodic commit and load csv") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
      writer.println("0")
      writer.println("3")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '$url' AS line " +
        s"CREATE ({name: 1/toInteger(line[0])})"

    // when executing 5 updates
    val e = intercept[Neo4jException](execute(queryText))

    val errorMessage = "csv' on line 3"
    // then
    e.getMessage should include(errorMessage)
    resourceMonitor.assertClosedAndClear(1)
  }

  test("should read committed properties in later transactions") {

    val csvFile = """name,flows,kbytes
                    |a,1,10.0
                    |b,1,10.0
                    |c,1,10.0
                    |d,1,10.0
                    |e,1,10.0
                    |f,1,10.0""".stripMargin

    val url = createFile(writer => writer.print(csvFile))

    val query = s"""CYPHER runtime=interpreted USING PERIODIC COMMIT 2 LOAD CSV WITH HEADERS FROM '$url' AS row
                   |MERGE (n {name: row.name})
                   |ON CREATE SET
                   |n.flows = toInteger(row.flows),
                   |n.kbytes = toFloat(row.kbytes)
                   |
                   |ON MATCH SET
                   |n.flows = n.flows + toInteger(row.flows),
                   |n.kbytes = n.kbytes + toFloat(row.kbytes)""".stripMargin

    def nodesWithFlow(i: Int) = s"MATCH (n {flows: $i}) RETURN count(*)"

    // Given
    execute(query)
    executeScalar[Int](nodesWithFlow(1)) should be(6)
    resourceMonitor.assertClosedAndClear(1)

    // When
    execute(query)

    // Then
    executeScalar[Int](nodesWithFlow(2)) should be(6)
    resourceMonitor.assertClosedAndClear(1)
  }
}
