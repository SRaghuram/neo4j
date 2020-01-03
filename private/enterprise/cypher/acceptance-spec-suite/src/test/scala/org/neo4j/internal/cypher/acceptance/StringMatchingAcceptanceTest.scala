/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class StringMatchingAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  var aNode: Node = _
  var bNode: Node = _
  var cNode: Node = _
  var dNode: Node = _
  var eNode: Node = _
  var fNode: Node = _

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
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name ENDS WITH 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should return null when CONTAINS is used on non-strings"){
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name CONTAINS 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should return null when CONTAINS is used on non-strings that contains integers") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name CONTAINS '1'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be(List())
  }

  test("should return null when STARTS WITH is used on non-strings"){
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name STARTS WITH 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should distinguish between one and multiple spaces in strings") {
    graph.withTx( tx => {
      tx.execute("CREATE (:Label{prop:'1 2'})")
      tx.execute("CREATE (:Label{prop:'1  2'})")
    })

    val result = executeSingle("MATCH (n:Label) RETURN size(n.prop) as l", Map.empty)
    result.toSet should equal(Set(Map("l" -> 3), Map("l" -> 4)))
  }

  test("should allow newline in label") {
    executeSingle(s"CREATE (:`Label with $newline in it`)")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, s"MATCH (n:`Label with $newline in it`) RETURN labels(n) as labels")
    result.toList should be(List(Map("labels" -> List(s"Label with $newline in it"))))
  }

  test("should allow newline in relationship type") {
    executeSingle(s"CREATE ()-[:`RELTYPE WITH $newline IN IT`]->()")

    val result = executeWith(Configs.All, s"MATCH ()-[r:`RELTYPE WITH $newline IN IT`]->() RETURN type(r) as type")
    result.toList should be(List(Map("type" -> s"RELTYPE WITH $newline IN IT")))
  }

  test("should allow newline in property key") {
    executeSingle(s"CREATE ({`prop name with $newline in it`: 1})")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, s"MATCH (n {`prop name with $newline in it`: 1}) RETURN keys(n) as keys")
    result.toList should be(List(Map("keys" -> List(s"prop name with $newline in it"))))
  }

  private val newline = if (System.getProperty("os.name").toLowerCase.startsWith("windows")) "\r\n" else "\n"
}
