/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.File

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher._
import org.neo4j.logging.AssertableLogProvider

class QueryEngineProceduresAcceptanceTest extends ExecutionEngineFunSuite {

  test("Clearing the query caches should work with empty caches") {
    val query = "CALL dbms.clearQueryCaches()"
    graph.inTx({
      val result = graph.execute(query)

      result.next().toString should equal ("{value=Query cache already empty.}")
      result.hasNext should be (false)
    })
  }

  test("Clearing the query caches should work properly") {
    val q1 = "MATCH (n) RETURN n.prop"
    val q2 = "MATCH (n) WHERE n.prop = 3 RETURN n"
    val q3 = "MATCH (n) WHERE n.prop = 4 RETURN n"

    graph.inTx({
      graph.execute(q1).close()
      graph.execute(q2).close()
      graph.execute(q3).close()
      graph.execute(q1).close()
    })

    graph.inTx({
      val query = "CALL dbms.clearQueryCaches()"
      val result = graph.execute(query)

      result.next().toString should equal ("{value=Query caches successfully cleared of 3 queries.}")
      result.hasNext should be (false)
    })
  }

  test("Multiple calls to clearing the cache should work") {
    val q1 = "MATCH (n) RETURN n.prop"

    graph.inTx(graph.execute(q1).close())

    graph.inTx({
      val query = "CALL dbms.clearQueryCaches()"
      val result = graph.execute(query)

      result.next().toString should equal ("{value=Query caches successfully cleared of 1 queries.}")
      result.hasNext should be (false)

      val result2 = graph.execute(query)
      result2.next().toString should equal ("{value=Query cache already empty.}")
      result2.hasNext should be (false)
    })
  }

  test("Test that cache clearing procedure writes to log") {
    val logProvider = new AssertableLogProvider()
    val managementService = graphDatabaseFactory(new File("test")).setUserLogProvider(logProvider).impermanent().build()
    val graphDatabaseService = managementService.database(DEFAULT_DATABASE_NAME)

    try {
      val transaction = graphDatabaseService.beginTx()
      graphDatabaseService.execute("MATCH (n) RETURN n.prop").close()
      transaction.commit()

      val tx = graphDatabaseService.beginTx()
      val query = "CALL dbms.clearQueryCaches()"
      graphDatabaseService.execute(query).close()
      tx.commit()

      logProvider.rawMessageMatcher().assertContains("Called dbms.clearQueryCaches(): Query caches successfully cleared of 1 queries.")
    }
    finally {
      managementService.shutdown()
    }

  }
}
