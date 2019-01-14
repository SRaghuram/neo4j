/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class RemoveAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("remove with case expression should work gh #10831") {
    // given
    graph.execute("CREATE (:Person {name: 'Alice', age: 23})-[:KNOWS]->(:Person {name:'Bob', age: 24})")

    // when
    val query =
      """MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
        |REMOVE CASE WHEN a.age>b.age THEN a ELSE b END.age
        |RETURN a.age, b.age""".stripMargin

    val result = executeWith(Configs.UpdateConf, query)

    // then
    result.toList should equal(List(Map("a.age" -> 23, "b.age" -> null)))
  }

  test("remove property from null literal") {
    executeWith(Configs.InterpretedAndSlotted - Configs.Cost2_3, "REMOVE null.p") should have size 0
  }

}
