/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class PruningVarExpandAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle query with no predicate") {
    // Given the graph:
    executeSingle("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL]->()-[:REL]->(:Molecule)""".stripMargin)

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (:Scaffold)-[:REL*3]->(m:Molecule)
        |RETURN DISTINCT m""".stripMargin)

    // Then
    result.toList should be(empty)
  }

  test("should handle query with predicates") {
    // Given the graph:
    executeSingle("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL {prop:index}]->()-[:REL {prop: index}]->(:Molecule)""".stripMargin)

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH ()-[:REL*2 {prop:42}]->(m) RETURN DISTINCT m""")

    // Then
    result.toList should have size 1
  }

  test("Pruning var expand should honour the predicate also for the first node") {
    createLabeledNode(Map("bar" -> 2), "Foo")
    val query =
      """
        |MATCH (a:Foo)
        |MATCH path = ( (a)-[:REL*0..2]-(b) )
        |WHERE ALL(n in nodes(path) WHERE n.bar = 1)
        |RETURN DISTINCT b
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList shouldBe empty
  }

  test("Pruning var expand should not break for unusual input node") {
    // Given the graph:
    val p =
      executeSingle(
        """CREATE (p:Person)
          |CREATE (p)-[:HAS_INTEREST]->(:Tag)
          |CREATE (p)-[:KNOWS]->()-[:KNOWS]->()
          |RETURN p""".stripMargin
      ).head("p")

    val query = """CALL {
                  |    MATCH (pX:Person)
                  |    RETURN pX
                  |}
                  |WITH collect(pX) AS pXs
                  |UNWIND pXs AS p1
                  |MATCH
                  |  (p1)-[:HAS_INTEREST]->(t:Tag),
                  |  (p1)-[:KNOWS*1..2]-(p2)
                  |RETURN p1, count(DISTINCT t) AS numTags""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList shouldBe List(Map("p1" -> p, "numTags" -> 1))
  }

  test("should respect predicate on start node") {

    //given
    relate(createLabeledNode("Movie", "Secure"), createLabeledNode("Person"))
    val query =
      """MATCH p=(a:Movie)-[*1..2]-(b:Person)
        |WHERE none(node IN nodes(p) WHERE node:Secure)
        |RETURN DISTINCT a, b LIMIT 10;""".stripMargin

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }
    ))

    //then
    result.toList should be(empty)
  }

  test("should handle ordered distinct") {
    //given
    executeSingle {
      """CREATE (a:A)
        |WITH a
        |UNWIND range(1, 10) AS i
        |CREATE (a)-[:REL]->()-[:REL]->(b:B)
        |CREATE (a)-[:REL]->()-[:REL]->(b)
        |""".stripMargin
    }

    val query =
      """MATCH (a:A)-[*1..2]-(b:B)
        |RETURN DISTINCT a, b
        |ORDER BY a""".stripMargin

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should {
          includeSomewhere.aPlan("OrderedDistinct") onTopOf
            includeSomewhere.aPlan("VarLengthExpand(Pruning)")
        }
      }
      ))

    //then
    result.toList.size shouldBe 10
  }

  test("should handle ordered aggregation") {
    //given

    val aNode = createLabeledNode("A")

    executeSingle {
      """MATCH (a:A)
        |WITH a
        |UNWIND range(1, 10) AS i
        |CREATE (a)-[:REL]->()-[:REL]->(b:B)
        |CREATE (a)-[:REL]->()-[:REL]->(b)
        |""".stripMargin
    }

    val query =
      """MATCH (a:A)-[*1..2]-(b:B)
        |RETURN a, count(DISTINCT b) as c
        |ORDER BY a""".stripMargin

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should {
          includeSomewhere.aPlan("OrderedAggregation") onTopOf
            includeSomewhere.aPlan("VarLengthExpand(Pruning)")
        }
      }
      ))

    //then
    result.toList shouldBe List(
      Map("a" -> aNode, "c" -> 10)
    )
  }
}
