/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class UnionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("Should work when doing union with same return variables") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "B")

    val query =
      """
        |MATCH (N:A)
        |RETURN
        |N.a as A,
        |N.b as B
        |UNION
        |MATCH (M:B) RETURN
        |M.b as A,
        |M.a as B
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val expected = List(Map("A" -> "a", "B" -> "b"), Map("A" -> "b", "B" -> "a"))

    result.toList should equal(expected)
  }

  test("Should work when doing union while mixing nodes and strings") {
    val node = createLabeledNode("A")

    val query1 =
      """
        |MATCH (n)
        |RETURN n AS A
        |UNION
        |RETURN "foo" AS A
      """.stripMargin

    val result1 = executeWith(Configs.InterpretedAndSlotted, query1)
    val expected1 = List(Map("A" -> node), Map("A" -> "foo"))

    result1.toList should equal(expected1)

    val query2 =
      """
        |RETURN "foo" AS A
        |UNION
        |MATCH (n)
        |RETURN n AS A
      """.stripMargin

    val result2 = executeWith(Configs.InterpretedAndSlotted, query2)
    val expected2 = List(Map("A" -> "foo"), Map("A" -> node))

    result2.toList should equal(expected2)
  }

  test("Should work when doing union while mixing nodes and relationships") {
    val node = createLabeledNode("A")
    val rel = relate(node, node, "T")

    val query1 =
      """
        |MATCH (n)
        |RETURN n AS A
        |UNION
        |MATCH ()-[r:T]->()
        |RETURN r AS A
      """.stripMargin

    val result1 = executeWith(Configs.InterpretedAndSlotted, query1)
    val expected1 = List(Map("A" -> node), Map("A" -> rel))

    result1.toList should equal(expected1)

    val query2 =
      """
        |MATCH ()-[r:T]->()
        |RETURN r AS A
        |UNION
        |MATCH (n)
        |RETURN n AS A
      """.stripMargin

    val result2 = executeWith(Configs.InterpretedAndSlotted, query2)
    val expected2 = List(Map("A" -> rel), Map("A" -> node))

    result2.toList should equal(expected2)
  }

  test("Should work when doing union of nodes in permuted order") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")

    val query =
      """
        |MATCH (N:A),(M:B)
        |RETURN
        |N, M
        |UNION
        |MATCH (N:B), (M:A) RETURN
        |M, N
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val expected = List(Map("M" -> b, "N" -> a), Map("M" -> a, "N" -> b))

    result.toList should equal(expected)
  }

  test("Should not work when doing union of differently names columns") {
    createLabeledNode("A")
    createLabeledNode("B")

    val query =
      """
        |MATCH (a:A),(b:B)
        |RETURN
        |a, b
        |UNION
        |MATCH (N:B), (M:A) RETURN
        |M, N
      """.stripMargin

    failWithError(Configs.All, query, message = Seq("All sub queries in an UNION must have the same column names"))
  }

  test("Should work when doing union with permutated return variables") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "b", "b" -> "a"), "B")

    val query =
      """
        |MATCH (N:A)
        |RETURN
        |N.a as B,
        |N.b as A
        |UNION
        |MATCH (M:B) RETURN
        |M.b as A,
        |M.a as B
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val expected = List(Map("A" -> "b", "B" -> "a"), Map("A" -> "a", "B" -> "b"))

    result.toList should equal(expected)
  }

  test("Should work when doing union ending in update") {

    val query =
      """
        |CREATE (a:A)
        |UNION
        |CREATE (b:B)
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val expected = List()

    result.toList should equal(expected)
  }
}
