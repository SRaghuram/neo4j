/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import org.neo4j.cypher.ExecutionEngineFunSuite

class ExecutionResultStatisticsTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

  test("correct statistics for added node property existence constraint") {
    val result = execute("create constraint on (n:Person) assert (n.name) is not null")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 1)
    assert(stats.existenceConstraintsRemoved === 0)
  }

  test("correct statistics for dropped node property existence constraint") {
    execute("create constraint on (n:Person) assert (n.name) is not null")
    val result = execute("drop constraint on (n:Person) assert exists(n.name)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 0)
    assert(stats.existenceConstraintsRemoved === 1)
  }

  test("correct statistics for added relationship property existence constraint") {
    val result = execute("create constraint on ()-[r:KNOWS]-() assert (r.since) is not null")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 1)
    assert(stats.existenceConstraintsRemoved === 0)
  }

  test("correct statistics for dropped relationship property existence constraint") {
    execute("create constraint on ()-[r:KNOWS]-() assert (r.since) is not null")
    val result = execute("drop constraint on ()-[r:KNOWS]-() assert exists(r.since)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 0)
    assert(stats.existenceConstraintsRemoved === 1)
  }
}
