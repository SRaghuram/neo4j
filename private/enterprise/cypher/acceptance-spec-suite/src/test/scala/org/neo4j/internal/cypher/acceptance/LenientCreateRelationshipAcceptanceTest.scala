/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class LenientCreateRelationshipAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_lenient_create_relationship -> "true"
  )

  // No CLG decision on this AFAIK, so not TCK material
  test("should silently not CREATE relationship if start-point is missing") {
    graph.execute("CREATE (a), (b)")


    val result = executeWith(Configs.InterpretedAndSlotted, """MATCH (a), (b)
                                       |WHERE id(a)=0 AND id(b)=1
                                       |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
                                       |CREATE (b)-[:LINK_TO]->(a)
                                       |CREATE (c)-[r:MISSING_C]->(a)""".stripMargin)

    assertStats(result, relationshipsCreated = 1)
  }

  // No CLG decision on this AFAIK, so not TCK material
  test("should silently not CREATE relationship if end-point is missing") {
    graph.execute("CREATE (a), (b)")

    val result = executeWith(Configs.InterpretedAndSlotted, """MATCH (a), (b)
                                       |WHERE id(a)=0 AND id(b)=1
                                       |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
                                       |CREATE (b)-[:LINK_TO]->(a)
                                       |CREATE (a)-[r:MISSING_C]->(c)""".stripMargin)

    assertStats(result, relationshipsCreated = 1)
  }

  // No CLG decision on this AFAIK, so not TCK material
  test("should silently not MERGE relationship if start-point is missing") {

    val result = executeWith(Configs.InterpretedAndSlotted,
      """MERGE (n:Node {Ogrn: "4"})
        |WITH n
        |OPTIONAL MATCH (m:Node { Ogrn: "4"}) WHERE id(n) <> id(m)
        |MERGE (m)-[:HasSameOgrn]->(n)""".stripMargin)

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

  // No CLG decision on this AFAIK, so not TCK material
  test("should silently not MERGE relationship if end-point is missing") {

    val result = executeWith(Configs.InterpretedAndSlotted,
      """MERGE (n:Node {Ogrn: "4"})
        |WITH n
        |OPTIONAL MATCH (m:Node { Ogrn: "4"}) WHERE id(n) <> id(m)
        |MERGE (n)-[:HasSameOgrn]->(m)""".stripMargin)

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

}
