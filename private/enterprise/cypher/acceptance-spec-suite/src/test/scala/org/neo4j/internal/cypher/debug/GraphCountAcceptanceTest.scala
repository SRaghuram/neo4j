/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import java.io.File

import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.neo4j.cypher.internal.compatibility.v4_0.Cypher4_0Planner
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.v4_0.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.collector.DataCollectorMatchers._
import org.neo4j.internal.collector.SampleGraphs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class GraphCountAcceptanceTest extends ExecutionEngineFunSuite
                               with QueryStatisticsTestSupport
                               with CypherComparisonSupport
                               with CreateGraphFromCounts
                               with SampleGraphs {

  ignore("template for support cases") {
    val file = new File("/.../graphCounts.json")
    val graphCounts = GraphCountsJson.parse(file)
    val row = graphCounts.results.head.data.head.row
    Cypher4_0Planner.customPlanContextCreator = Some((tc: TransactionalContextWrapper, logger: InternalNotificationLogger) => new GraphCountsPlanContext(row)(tc, logger))

    createGraph(graphCounts)

    // Modify graph to account for predicates in the query, add relationships, etc.

    // Execute your buggy query
  }

  test("should create graph from data collector graph counts")  {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    // Given
    createSteelfaceGraph()
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single
    val json = Serialization.write(List(res("section"), res("data")))
    // Put Rest API stuff around it
    val restJson = s"""
      |{
      |  "results": [
      |    {
      |      "columns": [
      |        "section",
      |        "data"
      |      ],
      |      "data": [
      |        {
      |          "row": $json
      |        }
      |      ]
      |    }
      |  ],
      |  "errors": []
      |}
    """.stripMargin

    // When
    // Nuke the previous graph
    restartWithConfig()
    val graphCounts = GraphCountsJson.parse(restJson)
    createGraph(graphCounts)

    // Then

    // Constraints
    executeSingle("CALL db.constraints").toList should be(
      List(
        Map("description" -> "CONSTRAINT ON ( user:User ) ASSERT user.email IS UNIQUE")
      )
    )

    // Indexes
    executeSingle("CALL db.indexes").toList should beListWithoutOrder(
      beMapContaining(
        "description" -> "INDEX ON :User(email)"
      ),
      beMapContaining(
        "description" -> "INDEX ON :User(lastName)"
      ),
      beMapContaining(
        "description" -> "INDEX ON :User(firstName, lastName)"
      ),
      beMapContaining(
        "description" -> "INDEX ON :Room(hotel, number)"
      ),
      beMapContaining(
        "description" -> "INDEX ON :Car(number)"
      )
    )

    // Nodes
    executeSingle("MATCH (n:User) RETURN n").toList should not be empty
    executeSingle("MATCH (n:Car) RETURN n").toList should not be empty
    executeSingle("MATCH (n:Room) RETURN n").toList should not be empty

    // Relationships
    // We have only statistics for one label + type, thus no guarantee that both labels plus type exists
    executeSingle("MATCH (u:User)-[:OWNS]->(n) RETURN u, n").toList should not be empty
    executeSingle("MATCH (u:User)-[:STAYS_IN]->(n) RETURN u, n").toList should not be empty
    executeSingle("MATCH (u)-[:OWNS]->(n:Car) RETURN u, n").toList should not be empty
    executeSingle("MATCH (u)-[:OWNS]->(n:Room) RETURN u, n").toList should not be empty
    executeSingle("MATCH (u)-[:STAYS_IN]->(n:Room) RETURN u, n").toList should not be empty
  }
}
