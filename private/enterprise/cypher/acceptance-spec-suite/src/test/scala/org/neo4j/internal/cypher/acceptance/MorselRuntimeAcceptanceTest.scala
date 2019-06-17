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

  test("should warn that runtime=parallel is experimental") {
    //Given
    import scala.collection.JavaConverters._

    val result = graph.execute("CYPHER runtime=parallel EXPLAIN MATCH (n) RETURN n")

    // When (exhaust result)
    val notifications = result.getNotifications.asScala.toSet

    //Then
    notifications.head.getDescription should equal("You are using an experimental feature (The parallel runtime is " +
                                                   "experimental and might suffer from instability and potentially correctness issues.)")

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
    val result = graph.execute("CYPHER runtime=parallel MATCH (n:N)-[:R]->(m:M)-[:P]->(o:O) RETURN o.i, o.j, o.k")

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

  // this is fixed in the follow up PR
  ignore("should big expand query without crashing") {
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

    val result = graph.execute("CYPHER runtime=parallel MATCH (n1:N)--(m1)--(n2)--(m2) RETURN id(m2)")

    var count = 0L
    result.accept((_: Result.ResultRow) => {
      count += 1L
      true
    })
    count should be(756900)
  }

  ignore("don't stall for nested plan expressions") {
    // Given
    graph.execute( """CREATE (a:A)
                     |CREATE (a)-[:T]->(:B),
                     |       (a)-[:T]->(:C)""".stripMargin)


    // When
    val result =
      graph.execute( """ CYPHER runtime=parallel
                       | MATCH (n)
                       | RETURN CASE
                       |          WHEN id(n) >= 0 THEN (n)-->()
                       |          ELSE 42
                       |        END AS p""".stripMargin)

    // Then
    asScalaResult(result).toList should not be empty
  }
}

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
