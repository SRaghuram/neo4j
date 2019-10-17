/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import java.io.File

import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.neo4j.cypher.internal.logical.plans.{CypherValue, FieldSignature, QualifiedName, UserFunctionSignature}
import org.neo4j.cypher.internal.planning.CypherPlanner
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

    // If your json has the correct format including boiler plate:
    val graphCounts = GraphCountsJson.parseAsGraphCountsJson(file)
    val graphCountData = graphCounts.results.head.data.head.row.data

    // If your json is missing the boiler plate, you can try this instead:
    //val graphCountData = GraphCountsJson.parseAsGraphCountData(file)

    def getPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger): GraphCountsPlanContext = {
      val context = new GraphCountsPlanContext(graphCountData)(tc, logger)
      // Add UDFs here, if you have any in your query

      // Add UDFs for temporal functions
      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "datetime"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(CypherValue( "DEFAULT_TEMPORAL_ARGUMENT", CTAny))
        )),
        CTDateTime, None, Array.empty, Some("Create a DateTime instant"), isAggregate = false,
        getUserFunctionHandle("datetime").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "date"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(CypherValue( "DEFAULT_TEMPORAL_ARGUMENT", CTAny))
        )),
        CTDate, None, Array.empty, Some("Create a Date instant"), isAggregate = false,
        getUserFunctionHandle("date").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "time"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(CypherValue( "DEFAULT_TEMPORAL_ARGUMENT", CTAny))
        )),
        CTTime, None, Array.empty, Some("Create a Time instant"), isAggregate = false,
        getUserFunctionHandle("time").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "localdatetime"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(CypherValue( "DEFAULT_TEMPORAL_ARGUMENT", CTAny))
        )),
        CTLocalDateTime, None, Array.empty, Some("Create a LocalDateTime instant"), isAggregate = false,
        getUserFunctionHandle("localdatetime").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "localtime"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(CypherValue( "DEFAULT_TEMPORAL_ARGUMENT", CTAny))
        )),
        CTLocalTime, None, Array.empty, Some("Create a LocalTime instant"), isAggregate = false,
        getUserFunctionHandle("localtime").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "duration"),
        IndexedSeq(FieldSignature("Input", CTAny)),
        CTDuration, None, Array.empty, Some("Construct a Duration value"), isAggregate = false,
        getUserFunctionHandle("duration").id())
      )

      context
    }

    CypherPlanner.customPlanContextCreator = Some(getPlanContext)

    createGraph(graphCountData)

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
