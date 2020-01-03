/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, GraphIcing}
import org.neo4j.internal.collector.DataCollectorMatchers._

class DataCollectorEnterpriseAcceptanceTest extends ExecutionEngineFunSuite with GraphIcing with EnterpriseGraphDatabaseTestSupport {

  test("should retrieve node existence constraints") {
    // Given
    execute("CREATE CONSTRAINT ON (n:User) ASSERT EXISTS (n.name)")

    // When
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')")

    // Then
    res.single should beMapContaining(
      "section" -> "GRAPH COUNTS",
      "data" -> beMapContaining(
        "constraints" -> beListWithoutOrder(
          beMapContaining(
            "label" -> "User",
            "properties" -> Seq("name"),
            "type" -> "Existence constraint"
          )
        )
      )
    )
  }
  test("should retrieve node key constraints") {
    // Given
    execute("CREATE CONSTRAINT ON (n:User) ASSERT (n.name, n.surname) IS NODE KEY")

    // When
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')")

    // Then
    res.single should beMapContaining(
      "section" -> "GRAPH COUNTS",
      "data" -> beMapContaining(
        "constraints" -> beListWithoutOrder(
          beMapContaining(
            "label" -> "User",
            "properties" -> Seq("name", "surname"),
            "type" -> "Node Key"
          )
        )
      )
    )
  }

  test("should retrieve relationship existence constraints") {
    // Given
    execute("CREATE CONSTRAINT ON ()-[n:User]-() ASSERT EXISTS (n.name)")

    // When
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')")

    // Then
    res.single should beMapContaining(
      "section" -> "GRAPH COUNTS",
      "data" -> beMapContaining(
        "constraints" -> beListWithoutOrder(
          beMapContaining(
            "relationshipType" -> "User",
            "properties" -> Seq("name"),
            "type" -> "Existence constraint"
          )
        )
      )
    )
  }
}
