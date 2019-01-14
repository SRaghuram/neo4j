/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class StringMatchingAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  val expectedToSucceed = Configs.InterpretedAndSlotted - Configs.Version2_3

  var aNode: Node = null
  var bNode: Node = null
  var cNode: Node = null
  var dNode: Node = null
  var eNode: Node = null
  var fNode: Node = null

  override def initTest() {
    super.initTest()
    aNode = createLabeledNode(Map("name" -> "ABCDEF"), "LABEL")
    bNode = createLabeledNode(Map("name" -> "AB"), "LABEL")
    cNode = createLabeledNode(Map("name" -> "abcdef"), "LABEL")
    dNode = createLabeledNode(Map("name" -> "ab"), "LABEL")
    eNode = createLabeledNode(Map("name" -> ""), "LABEL")
    fNode = createLabeledNode("LABEL")
  }

  test("should return null when END WITH is used on non-strings"){
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name ENDS WITH 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should return null when CONTAINS is used on non-strings"){
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name CONTAINS 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should return null when CONTAINS is used on non-strings that contains integers") {
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name CONTAINS '1'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be(List())
  }

  test("should return null when STARTS WITH is used on non-strings"){
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name STARTS WITH 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should distinguish between one and multiple spaces in strings") {
    graph.execute("CREATE (:Label{prop:'1 2'})")
    graph.execute("CREATE (:Label{prop:'1  2'})")

    val result = executeSingle("MATCH (n:Label) RETURN length(n.prop) as l", Map.empty)
    result.toSet should equal(Set(Map("l" -> 3), Map("l" -> 4)))
  }
}
