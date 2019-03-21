/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.{Callable, Executors, Future}

import org.neo4j.cypher.ExecutionEngineFunSuite

class SameQueryStressTest extends ExecutionEngineFunSuite {

  private val lookup = """MATCH (theMatrix:Movie {title:'The Matrix'})<-[:ACTED_IN]-(actor)-[:ACTED_IN]->(other:Movie)
                          WHERE other <> theMatrix
                          RETURN actor""".stripMargin

  for {
    runtime <- List("interpreted", "slotted", "compiled")
  } testRuntime(runtime)

  private def testRuntime(runtime: String): Unit = {

    test(s"concurrent query execution in $runtime") {

      // Given
      graph.execute(TestGraph.movies)
      val expected = graph.execute(lookup).resultAsString()

      // When
      val nThreads = 10
      val executor = Executors.newFixedThreadPool(nThreads)
      val futureResultsAsExpected: Seq[Future[Array[String]]] =
        for (_ <- 1 to nThreads) yield
          executor.submit(new Callable[Array[String]] {
            override def call(): Array[String] = {
              (for (_ <- 1 to 1000) yield {
                graph.execute(lookup).resultAsString()
              }).toArray
            }
          })

      // Then no crashes...
      for (futureResult: Future[Array[String]] <- futureResultsAsExpected;
           result: String <- futureResult.get
      ) {
        // ...and correct results
        result should equal(expected)
      }
      executor.shutdown()
    }
  }
}

