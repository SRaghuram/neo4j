/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import java.io.File

import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.v4_0.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.collector.DataCollectorMatchers._
import org.neo4j.internal.collector.SampleGraphs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

import scala.collection.immutable

class GraphCountAcceptanceTest extends ExecutionEngineFunSuite
                               with QueryStatisticsTestSupport
                               with CypherComparisonSupport
                               with CreateGraphFromCounts
                               with SampleGraphs {

  ignore("template for support cases") {
    val file = new File("/.../graphCounts.json")
    val graphCounts = GraphCountsJson.parse(file)
    val row = graphCounts.results.head.data.head.row

    def getPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger): GraphCountsPlanContext = {
      val context = new GraphCountsPlanContext(row)(tc, logger)
      // Add UDFs here, if you have any in your query
      context
    }

    CypherPlanner.customPlanContextCreator = Some(getPlanContext)

    createGraph(graphCounts)

    // Modify graph to account for predicates in the query, add relationships, etc.

    // Execute your buggy query
    val query = ???

    graph.withTx(tx => {
      val r = tx.execute(query)
      println(r.resultAsString())
      println(r.getExecutionPlanDescription)
    })
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
        Map(
          "name" -> "Uniqueness constraint on :User (email)",
          "description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.email) IS UNIQUE")
      )
    )

    // Indexes
    executeSingle("CALL db.indexes").toList should beListWithoutOrder(
      beMapContaining(
        "labelsOrTypes" -> List("User"),
        "properties" -> List("email"),
        "type" -> "BTREE"
      ),
      beMapContaining(
        "labelsOrTypes" -> List("User"),
        "properties" -> List("lastName"),
          "type" -> "BTREE"
      ),
      beMapContaining(
        "labelsOrTypes" -> List("User"),
        "properties" -> List("firstName", "lastName"),
        "type" -> "BTREE"
      ),
      beMapContaining(
        "labelsOrTypes" -> List("Room"),
        "properties" -> List("hotel", "number"),
        "type" -> "BTREE"
      ),
      beMapContaining(
        "labelsOrTypes" -> List("Car"),
        "properties" -> List("number"),
        "type" -> "BTREE"
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
