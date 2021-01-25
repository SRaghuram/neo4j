/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.scalatest.Matchers

class ToStringAcceptanceTest extends ExecutionEngineFunSuite with Matchers with EnterpriseGraphDatabaseTestSupport {

  test("Node should provide sensible toString") {
    val data = makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH (a:A) RETURN a")
      result.columnAs("a").next().toString should be(s"Node[${data("a")}]")
    })
  }

  test("Node should provide sensible toString within transactions") {
    val data = makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH (a:A) RETURN a")
      result.columnAs("a").next().toString should be(s"Node[${data("a")}]")
    } )
  }

  test("Node collection should provide sensible toString") {
    makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH (a) RETURN collect(a) as nodes")
      result.columnAs("nodes").next().toString should fullyMatch regex "\\[Node\\[\\d+\\], Node\\[\\d+\\], Node\\[\\d+\\]\\]"
    })
  }

  test("Relationship should provide sensible toString") {
    val data = makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH (:A)-[r]->(:B) RETURN r")
      result.columnAs("r").next().toString should be(s"(0)-[KNOWS,${data("ab")}]->(1)")
    })
  }

  test("Relationship should provide sensible toString within transactions") {
    val data = makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH (:A)-[r]->(:B) RETURN r")
      result.columnAs("r").next().toString should be(s"(${data("a")})-[KNOWS,${data("ab")}]->(${data("b")})")
    } )
  }

  test("Path should provide sensible toString") {
    val data = makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH p = (:A)-->(:B)-->(:C) RETURN p")
      result.columnAs("p").next().toString should (
        be(s"(?)-[?,${data("ab")}]-(?)-[?,${data("bc")}]-(?)") or
          be(s"(${data("a")})-[KNOWS,${data("ab")}]->(${data("b")})-[KNOWS,${data("bc")}]->(${data("c")})"))
    })
  }

  test("Path should provide sensible toString within transactions") {
    val data = makeModel()
    graph.withTx( tx => {
      val result = tx.execute("MATCH p = (:A)-->(:B)-->(:C) RETURN p")
      result.columnAs("p").next().toString should (
        be(s"(${data("a")})-[KNOWS,${data("ab")}]->(${data("b")})-[KNOWS,${data("bc")}]->(${data("c")})") or
          be(s"(${data("a")})-[${data("ab")}:KNOWS]->(${data("b")})-[${data("bc")}:KNOWS]->(${data("c")})"))
    } )
  }

  private def makeModel() = {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val c = createLabeledNode("C")
    val ab = relate(a, b, "KNOWS")
    val bc = relate(b, c, "KNOWS")
    Map("a" -> a.getId, "b" -> b.getId, "c" -> c.getId, "ab" -> ab.getId, "bc" -> bc.getId)
  }
}
