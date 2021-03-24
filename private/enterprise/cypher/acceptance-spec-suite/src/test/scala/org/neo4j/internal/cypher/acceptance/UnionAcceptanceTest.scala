/*
 * Copyright (c) "Neo4j"
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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
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

    val result1 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query1)
    val expected1 = List(Map("A" -> node), Map("A" -> "foo"))

    result1.toList should equal(expected1)

    val query2 =
      """
        |RETURN "foo" AS A
        |UNION
        |MATCH (n)
        |RETURN n AS A
      """.stripMargin

    val result2 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query2)
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

    val result1 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query1)
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

    val result2 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query2)
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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
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

    failWithError(Configs.All, query, message = "All sub queries in an UNION must have the same column names")
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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val expected = List()

    result.toList should equal(expected)
  }

  test("Should use ordered union to solve label disjunction") {
    for(i <- 0 until 300) {
      val labels = Seq(
        Option("A").filter(_ => i < 100),
        Option("B").filter(_ => i % 10 == 0),
        Option("C").filter(_ => i % 8 == 0),
      ).flatten
      createLabeledNode(labels: _*)
    }

    val result = executeSingle("MATCH (n) WHERE n:A or n:B RETURN labels(n) AS l")
    result.executionPlanDescription() should includeSomewhere
      .aPlan("OrderedDistinct").withEstimatedRows(120)
      .onTopOf(aPlan("OrderedUnion").withEstimatedRows(130))
    result.columnAs[Seq[String]]("l").foreach { labels =>
      labels should (contain("A") or contain("B"))
    }
    result.size should be(120)
  }

  test("union subQuery with renaming") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    relate(nodeA, nodeB)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x {name:'A'})
        |CALL {
        |  WITH x RETURN x AS y
        |  UNION
        |  WITH x MATCH (x)-[]-(y) RETURN y
        |}
        |WITH y AS z
        |RETURN z.name, id(z)
        |ORDER BY z.name asc""".stripMargin)

    result.executionPlanDescription() should not(includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty(".*", ".*"))
    result.toList should equal(List(Map("z.name" -> "A", "id(z)" -> nodeA.getId), Map("z.name" -> "B", "id(z)" -> nodeB.getId)))
  }

  test("union subQuery identical renaming with caching") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    relate(nodeA, nodeB)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x {name:'A'})
        |CALL {
        |  WITH x RETURN x as y
        |  UNION
        |  WITH x RETURN x as y
        |}
        |WITH y AS z
        |RETURN z.name, id(z)
        |ORDER BY z.name asc""".stripMargin)

    result.executionPlanDescription() should includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty("x", "name")
    result.executionPlanDescription() should includeSomewhere .aPlan("Projection").containingArgumentForCachedProperty("z", "name")
    result.toList should equal(List(Map("z.name" -> "A", "id(z)" -> nodeA.getId)))
  }

  test("union subQuery with renaming without cached projection property") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    relate(nodeA, nodeB)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x {name:'A'})
        |CALL {
        |  WITH x MATCH (x)-[]-(y) where y.name='B' RETURN y
        |  UNION
        |  WITH x MATCH (x)-[]-(z) where x.name='A' RETURN x as y
        |}
        |WITH y AS z
        |RETURN z.name, id(z)
        |ORDER BY z.name asc""".stripMargin)
    result.executionPlanDescription() should not(includeSomewhere .aPlan("Projection").containingArgumentForCachedProperty("z", "name"))
    result.executionPlanDescription() should includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty("x", "name")
    result.toList should equal(List(Map("z.name" -> "A", "id(z)" -> nodeA.getId), Map("z.name" -> "B", "id(z)" -> nodeB.getId)))
  }

  test("union subQuery with rename with reverse order") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    relate(nodeA, nodeB)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x {name:'A'})
        |CALL {
        |  WITH x MATCH (x)-[]-(y) RETURN y
        |  UNION
        |  WITH x RETURN x AS y
        |}
        |WITH y AS z
        |RETURN z.name, id(z)
        |ORDER BY z.name asc""".stripMargin)

    result.executionPlanDescription() should not(includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty(".*", ".*"))
    result.toList should equal(List(Map("z.name" -> "A", "id(z)" -> nodeA.getId), Map("z.name" -> "B", "id(z)" -> nodeB.getId)))
  }

  test("union subQuery without renaming") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    relate(nodeA, nodeB)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x {name:'A'})
        |CALL {
        |  WITH x RETURN x AS y
        |  UNION
        |  WITH x MATCH (x)-[]-(y) RETURN y
        |}
        |WITH y
        |RETURN y.name, id(y)
        |ORDER BY y.name asc""".stripMargin)

    result.executionPlanDescription() should not(includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty(".*", ".*"))
    result.toList should equal(List(Map("y.name" -> "A", "id(y)" -> 0), Map("y.name" -> "B", "id(y)" -> 1)))
  }

  test("union subQuery with renaming and without caching") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    relate(nodeA, nodeB)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x)
        |CALL {
        |  WITH x RETURN x AS y
        |  UNION
        |  WITH x MATCH (x)-[]-(y) RETURN y
        |}
        |WITH y as z
        |RETURN z.name, id(z)
        |ORDER BY z.name asc""".stripMargin)

    result.executionPlanDescription() should not(includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty(".*", ".*"))
    result.toList should equal(List(Map("z.name" -> "A", "id(z)" -> nodeA.getId),
      Map("z.name" -> "A", "id(z)" -> 0),
      Map("z.name" -> "B", "id(z)" -> 1),
      Map("z.name" -> "B", "id(z)" -> 1)))
  }

  test("recursive union subQueries") {
    val nodeA = createLabeledNode(Map("name" -> "A"), "Person")
    val nodeB = createLabeledNode(Map("name" -> "B"), "Person")
    val nodeC = createLabeledNode(Map("name" -> "C"), "Person")
    relate(nodeA, nodeB, "KNOWS")
    relate(nodeB, nodeC, "LIKES")
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (x {name:'B'})
        |CALL {
        |  WITH x CALL {
        |     WITH x RETURN x as y
        |     UNION
        |     WITH x MATCH (x)-[:LIKES]-(y) RETURN y
        |  } WITH y RETURN y
        |  UNION
        |  WITH x MATCH (y)-[:KNOWS]-(a) RETURN y
        |}
        |WITH y
        |RETURN y.name, id(y)
        |ORDER BY y.name asc""".stripMargin)

    result.executionPlanDescription() should not(includeSomewhere .aPlan("Filter").containingArgumentForCachedProperty(".*", ".*"))
    result.toList should equal(List(Map("y.name" -> "A", "id(y)" -> nodeA.getId), Map("y.name" -> "B", "id(y)" -> nodeB.getId), Map("y.name" -> "C", "id(y)" -> nodeC.getId)))
  }
}
