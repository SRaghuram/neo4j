/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future

import org.neo4j.cypher.ExecutionEngineFunSuite

class SameQueryStressTest extends ExecutionEngineFunSuite {

  private val lookup = """MATCH (theMatrix:Movie {title:'The Matrix'})<-[:ACTED_IN]-(actor)-[:ACTED_IN]->(other:Movie)
                          WHERE other <> theMatrix
                          RETURN actor
                          ORDER BY actor.born
                          """.stripMargin

  for {
    runtime <- List("interpreted", "slotted", "pipelined")
  } testRuntime(runtime)

  private def testRuntime(runtime: String): Unit = {

    test(s"concurrent query execution in $runtime") {

      // Given
      var expected = ""
      val transaction = graphOps.beginTx()
      try {
        transaction.execute(TestGraph.movies).close()
        expected = transaction.execute(lookup).resultAsString()
        transaction.commit()
      } finally {
        transaction.close()
      }

      // When
      val nThreads = 10
      val executor = Executors.newFixedThreadPool(nThreads)
      val futureResultsAsExpected: Seq[Future[Array[String]]] =
        for (_ <- 1 to nThreads) yield
          executor.submit(new Callable[Array[String]] {
            override def call(): Array[String] = {
              (for (_ <- 1 to 1000) yield {
                val transaction = graphOps.beginTx()
                try {
                  transaction.execute(lookup).resultAsString()
                } finally {
                  transaction.close()
                }
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

