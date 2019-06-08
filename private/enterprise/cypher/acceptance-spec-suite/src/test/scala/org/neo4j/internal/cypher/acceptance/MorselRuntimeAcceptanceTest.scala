/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.{Node, Result}
import org.neo4j.internal.cypher.acceptance.MorselRuntimeAcceptanceTest.MORSEL_SIZE
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.Ignore

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object MorselRuntimeAcceptanceTest {
  val MORSEL_SIZE = 4 // The morsel size to use in the config for testing
}

abstract class MorselRuntimeAcceptanceTest extends ExecutionEngineFunSuite {

  private val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  def assertTolerantEquals(expected: Double, x: Any): Unit =
    x match {
      case n: Number =>
        assert(doubleEquality.areEqual(expected + 0.00001, n.doubleValue()))
      case _ => assert(false)
    }

  test("should not use morsel by default") {
    //Given
    val result = graph.execute("MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }

  test("should be able to ask for morsel") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should warn that morsels are experimental") {
    //Given
    import scala.collection.JavaConverters._

    val result = graph.execute("CYPHER runtime=morsel EXPLAIN MATCH (n) RETURN n")

    // When (exhaust result)
    val notifications = result.getNotifications.asScala.toSet

    //Then
    notifications.head.getDescription should equal("You are using an experimental feature (use the morsel runtime at " +
                                                     "your own peril, not recommended to be run on production systems)")

  }

  test("should produce results non-concurrently") {
    // Given a big network
    for (i <- 1 to 10) {
      val n = createLabeledNode("N")
      for (j <- 1 to 10) {
        val m = createLabeledNode("M")
        relate(n, m, "R")
        for (k <- 1 to 10) {
          val o = createLabeledNode(Map("i" -> i, "j" -> j, "k" -> k), "O")
          relate(m, o, "P")
        }
      }
    }

    val switch = new AtomicBoolean(false)

    // When executing a query that has multiple ProduceResult tasks
    val result = graph.execute("CYPHER runtime=morsel MATCH (n:N)-[:R]->(m:M)-[:P]->(o:O) RETURN o.i, o.j, o.k")

    // Then these tasks should be executed non-concurrently
    result.accept(new ResultVisitor[Exception]() {
      override def visit(row: Result.ResultRow): Boolean = {
        if (!switch.compareAndSet(false, true)) {
          fail("Expected switch to be false: Concurrently doing ProduceResults.")
        }
        Thread.sleep(0)
        if (!switch.compareAndSet(true, false)) {
          fail("Expected switch to be true: Concurrently doing ProduceResults.")
        }
        true
      }
    })
  }

  test("should big expand query without crashing") {
    // Given a big network
    val SIZE = 30
    val ns = new ArrayBuffer[Node]()
    val ms = new ArrayBuffer[Node]()

    graph.inTx {
      for (_ <- 1 to SIZE) ns += createLabeledNode("N")
      for (_ <- 1 to SIZE) ms += createLabeledNode("M")
    }

    graph.inTx {
      for {n <- ns; m <- ms} {
        relate(n, m, "R")
      }
    }

    val result = graph.execute("CYPHER runtime=morsel MATCH (n1:N)--(m1)--(n2)--(m2) RETURN id(m2)")

    var count = 0L
    while (result.hasNext) {
      result.next()
      count += 1
    }
    count should be(756900)
  }

  ignore("don't stall for nested plan expressions") {
    // Given
    graph.execute( """CREATE (a:A)
                     |CREATE (a)-[:T]->(:B),
                     |       (a)-[:T]->(:C)""".stripMargin)


    // When
    val result =
      graph.execute( """ CYPHER runtime=morsel
                       | MATCH (n)
                       | RETURN CASE
                       |          WHEN id(n) >= 0 THEN (n)-->()
                       |          ELSE 42
                       |        END AS p""".stripMargin)

    // Then
    asScalaResult(result).toList should not be empty
  }
}

@Ignore
class ParallelMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "0"
  )
}

class SingleThreadedMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "1"
  )
}

/**
  * These tests can only be run with cypher_hints_error = false
  * since they test with queries that are not supported.
  */
class MorselRuntimeNotSupportedAcceptanceTest extends ExecutionEngineFunSuite {
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "false",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "0"
  )

  test("should fallback if morsel doesn't support query") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n)-[*]->(m) RETURN n SKIP 1")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }
}
