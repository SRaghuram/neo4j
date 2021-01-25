/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE
import java.util.concurrent.atomic.AtomicBoolean

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.cypher.acceptance.ParallelRuntimeAcceptanceTest.MORSEL_SIZE
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object ParallelRuntimeAcceptanceTest {
  val MORSEL_SIZE = 4 // The morsel size to use in the config for testing
}

class ParallelRuntimeAcceptanceTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(MORSEL_SIZE),
    GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(MORSEL_SIZE),
  )

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
    graph.withTx( tx => {
      val result = tx.execute("CYPHER runtime=parallel MATCH (n:N)-[:R]->(m:M)-[:P]->(o:O) RETURN o.i, o.j, o.k")

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
    })
  }

  test("should warn that runtime=parallel is experimental") {
    //Given

    val result = graph.withTx( tx => tx.execute("CYPHER runtime=parallel EXPLAIN MATCH (n) RETURN n"))

    // When (exhaust result)
    val notifications = result.getNotifications.asScala.toSet

    //Then
    notifications.head.getDescription should equal("You are using an experimental feature (The parallel runtime is " +
      "experimental and might suffer from instability and potentially correctness issues.)")

  }
}

class NoWorkersParallelRuntimeTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseInternalSettings.cypher_worker_count -> java.lang.Integer.valueOf(-1)
  )

  test("should not use parallel runtime if there are no workers") {
    the[RuntimeUnsupportedException] thrownBy {
      executeSingle("CYPHER runtime=parallel MATCH (n) RETURN n")
    } should have message "There are no workers configured for the parallel runtime. Change 'unsupported.cypher.number_of_workers' to something other than -1 to use the parallel runtime."
  }
}
