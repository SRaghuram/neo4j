/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.nio.file.Path

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.LogAssertions.assertThat

class QueryEngineProceduresAcceptanceTest extends ExecutionEngineFunSuite {

  test("Clearing the query caches should work with empty caches") {
    val query = "CALL db.clearQueryCaches()"
    graph.withTx( tx => {
      val result = tx.execute(query)

      result.next().toString should equal ("{value=Query cache already empty.}")
      result.hasNext should be (false)
    })
  }

  test("Clearing the query caches should work properly") {
    val q1 = "MATCH (n) RETURN n.prop"
    val q2 = "MATCH (n) WHERE n.prop = 3 RETURN n"
    val q3 = "MATCH (n) WHERE n.prop = 4 RETURN n"

    graph.withTx( tx => {
      tx.execute(q1).close()
      tx.execute(q2).close()
      tx.execute(q3).close()
      tx.execute(q1).close()
    })

    graph.withTx( tx => {
      val query = "CALL db.clearQueryCaches()"
      val result = tx.execute(query)

      result.next().toString should equal ("{value=Query caches successfully cleared of 3 queries.}")
      result.hasNext should be (false)
    })
  }

  test("Multiple calls to clearing the cache should work") {
    val q1 = "MATCH (n) RETURN n.prop"

    graph.withTx( tx => tx.execute(q1).close())

    graph.withTx( tx => {
      val query = "CALL db.clearQueryCaches()"
      val result = tx.execute(query)

      result.next().toString should equal ("{value=Query caches successfully cleared of 1 queries.}")
      result.hasNext should be (false)

      val result2 = tx.execute(query)
      result2.next().toString should equal ("{value=Query cache already empty.}")
      result2.hasNext should be (false)
    })
  }

  test("Test that cache clearing procedure writes to log") {
    val logProvider = new AssertableLogProvider()
    val managementService = graphDatabaseFactory(Path.of("test")).setUserLogProvider(logProvider).impermanent().build()
    val graphDatabaseService = managementService.database(DEFAULT_DATABASE_NAME)

    try {
      val transaction = graphDatabaseService.beginTx()
      transaction.execute("MATCH (n) RETURN n.prop").close()
      transaction.commit()

      val tx = graphDatabaseService.beginTx()
      val query = "CALL db.clearQueryCaches()"
      tx.execute(query).close()
      tx.commit()

      assertThat(logProvider).containsMessages("Called db.clearQueryCaches(): Query caches successfully cleared of 1 queries.")
    }
    finally {
      managementService.shutdown()
    }

  }
}
