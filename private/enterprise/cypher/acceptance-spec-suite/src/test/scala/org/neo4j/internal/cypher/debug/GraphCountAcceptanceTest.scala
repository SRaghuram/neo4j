/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import java.io.File

import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.StringInput
import org.json4s.native.JsonMethods
import org.json4s.native.Serialization
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.internal.collector.DataCollectorMatchers.beListWithoutOrder
import org.neo4j.internal.collector.DataCollectorMatchers.beMapContaining
import org.neo4j.internal.collector.SampleGraphs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.logging.Log
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

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

    def getPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger, log: Log): GraphCountsPlanContext = {
      val context = new GraphCountsPlanContext(graphCountData)(tc, logger)

      // Add UDFs here, if you have any in your query

      // Add UDFs for temporal functions
      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "datetime"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(stringValue( "DEFAULT_TEMPORAL_ARGUMENT"))
        )),
        CTDateTime, None, Array.empty, Some("Create a DateTime instant"), isAggregate = false,
        getUserFunctionHandle("datetime").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "date"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(stringValue( "DEFAULT_TEMPORAL_ARGUMENT"))
        )),
        CTDate, None, Array.empty, Some("Create a Date instant"), isAggregate = false,
        getUserFunctionHandle("date").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "time"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(stringValue( "DEFAULT_TEMPORAL_ARGUMENT"))
        )),
        CTTime, None, Array.empty, Some("Create a Time instant"), isAggregate = false,
        getUserFunctionHandle("time").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "localdatetime"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(stringValue( "DEFAULT_TEMPORAL_ARGUMENT"))
        )),
        CTLocalDateTime, None, Array.empty, Some("Create a LocalDateTime instant"), isAggregate = false,
        getUserFunctionHandle("localdatetime").id())
      )

      context.addUDF( UserFunctionSignature(QualifiedName(Seq.empty, "localtime"),
        IndexedSeq(FieldSignature("Input", CTAny,
          Some(stringValue( "DEFAULT_TEMPORAL_ARGUMENT"))
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
    println("Finished creating graph")

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
          "name" -> "constraint_3a8336b6",
          "description" -> "CONSTRAINT ON ( user:User ) ASSERT (user.email) IS UNIQUE",
          "details" -> "Constraint( id=6, name='constraint_3a8336b6', type='UNIQUENESS', schema=(:User {email}), ownedIndex=1 )")
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

  test("should use index backed order by with data collector graph counts and custom plan context") {
    val json = s"""
         |{
         |  "relationships":[],
         |  "nodes":[
         |    {"count":150},
         |    {"count":90,"label":"Person"}
         |  ],
         |  "indexes":[{"updatesSinceEstimation":0,"totalSize":0,"properties":["name"],"labels":["Person"],"estimatedUniqueSize":0}],
         |  "constraints":[]
         |}
         |""".stripMargin

    implicit val formats: Formats = DefaultFormats + RowSerializer
    val graphCountData = JsonMethods.parse(StringInput(json)).extract[GraphCountData]

    createGraph(graphCountData)

    val tx = graph.beginTransaction(EXPLICIT, AUTH_DISABLED)
    val transactionalContext = createTransactionContext(graph, tx)

    // Create a custom plan context like the one in the support case template above
    val planContext = new GraphCountsPlanContext(graphCountData)(TransactionalContextWrapper(transactionalContext), devNullLogger)

    try {
      // Index Seek code path
      val indexDescriptorIterator = planContext.indexesGetForLabel(0)
      indexDescriptorIterator.hasNext should be(true)
      val seekIndexDescriptor = indexDescriptorIterator.next()
      seekIndexDescriptor.orderCapability(Seq(CTString)) should be(BOTH)

      // Index Scan code path
      val maybeIndexDescriptor = planContext.indexGetForLabelAndProperties("Person", Seq("name"))
      maybeIndexDescriptor.isDefined should be(true)
      val scanIndexDescriptor = maybeIndexDescriptor.get
      scanIndexDescriptor.orderCapability(Seq(CTString)) should be(BOTH)

      transactionalContext.close()
      tx.commit()
    } catch {
      case t: Throwable =>
        transactionalContext.close()
        tx.rollback()
        throw t
    } finally {
      tx.close()
    }
  }

  private def createTransactionContext(graphDatabaseCypherService: GraphDatabaseQueryService, tx: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseCypherService)
    contextFactory.newContext(tx, "no query", EMPTY_MAP)
  }
}
