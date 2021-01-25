/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class LenientCreateRelationshipAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_lenient_create_relationship -> TRUE
  )

  // No CLG decision on this AFAIK, so not TCK material
  test("should silently not CREATE relationship if start-point is missing") {
    graph.withTx( tx => tx.execute("CREATE (a), (b)"))


    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
    """MATCH (a), (b)
      |WHERE id(a)=0 AND id(b)=1
      |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
      |CREATE (b)-[:LINK_TO]->(a)
      |CREATE (c)-[r:MISSING_C]->(a)""".stripMargin)

    assertStats(result, relationshipsCreated = 1)
  }

  test("should silently not CREATE relationship if start-point is missing and return null") {
    graph.withTx( tx => tx.execute("CREATE (a), (b)"))

    //List(Map(r1 -> (?)-[RELTYPE(0),0]->(?), r2 -> null))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (a), (b)
        |WHERE id(a)=0 AND id(b)=1
        |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
        |CREATE (b)-[r1:LINK_TO]->(a)
        |CREATE (c)-[r2:MISSING_C]->(a)
        |RETURN r2""".stripMargin)

    assertStats(result, relationshipsCreated = 1)
    result.toList should equal(List(Map("r2" -> null)))
  }

  // No CLG decision on this AFAIK, so not TCK material
  test("should silently not CREATE relationship if end-point is missing") {
    graph.withTx( tx => tx.execute("CREATE (a), (b)"))

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (a), (b)
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
