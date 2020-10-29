/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ShortestPathComplexQueryAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("allShortestPaths with complex LHS should not be planned with exhaustive fallback and inject predicate") {
    setupModel()
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |PROFILE MATCH (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |WITH kim AS kimDeal, collect(charles) AS charlesT, collect(joey) AS joeyS
        |UNWIND charlesT AS charlesThompson
        |UNWIND joeyS AS joeySantiago
        |MATCH pathx = allShortestPaths((charlesThompson)-[*1..5]-(joeySantiago))
        |WHERE none (n IN nodes(pathx) WHERE id(n) = id(kimDeal))
        |RETURN [node in nodes(pathx) | id(node)] as ids
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("AntiConditionalApply").withRHS(includeSomewhere.aPlan("VarLengthExpand(Into)"))))

    val results = result.columnAs("ids").toList
    results should be(List(List(0, 4, 3, 2)))
  }

  test("shortestPath with complex LHS should not be planned with exhaustive fallback and inject predicate") {
    setupModel()
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |PROFILE MATCH (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |WITH kim AS kimDeal, collect(charles) AS charlesT, collect(joey) AS joeyS
        |UNWIND charlesT AS charlesThompson
        |UNWIND joeyS AS joeySantiago
        |MATCH pathx = shortestPath((charlesThompson)-[*1..5]-(joeySantiago))
        |WHERE none (n IN nodes(pathx) WHERE id(n) = id(kimDeal))
        |RETURN [node in nodes(pathx) | id(node)] as ids
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("AntiConditionalApply").withRHS(includeSomewhere.aPlan("VarLengthExpand(Into)"))))

    val results = result.columnAs("ids").toList
    results should be(List(List(0, 4, 3, 2)))
  }

  private def setupModel(): Unit = {
    executeSingle(
      """
        |MERGE (p1:Pixie {fname:'Charles'})
        |MERGE (p2:Pixie {fname:'Kim'})
        |MERGE (p3:Pixie {fname:'Joey'})
        |MERGE (p4:Pixie {fname:'David'})
        |MERGE (p5:Pixie {fname:'Paz'})
        |MERGE (p1)-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4)-[:KNOWS]->(p5)-[:KNOWS]->(p1)
        |RETURN p1,p2,p3,p4,p5
      """.stripMargin)
  }
}
