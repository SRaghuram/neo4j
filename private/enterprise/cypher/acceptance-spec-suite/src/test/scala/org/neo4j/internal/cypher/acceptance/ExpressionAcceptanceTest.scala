/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle map projection with property selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n{.foo,.bar,.baz}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "baz" -> null))
  }

  test("should handle map projection with property selectors and identifier selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "WITH 42 as x MATCH (n) RETURN n{.foo,.bar,x}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "x" -> 42))
  }

  test("should use the map identifier as the alias for return items") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n{.foo,.bar}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection with all-properties selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n{.*}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection on an optional node and collect") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "OPTIONAL MATCH (n) RETURN collect(n{.*}) AS map")

    result.toList should equal(List(Map("map" -> List(Map("foo" -> 1, "bar" -> "apa")))))
  }

  test("map projection on an optional relationship and collect") {
    relate(createLabeledNode("START"), createNode(), "foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "OPTIONAL MATCH (start: START) OPTIONAL MATCH (start)-[r]->() RETURN collect(r{.*}) AS map")

    result.toList should equal(List(Map("map" -> List(Map("foo" -> 1, "bar" -> "apa")))))
  }

  test("returning all properties of a node and adds other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n{.*, .baz}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("returning all properties of a node and overwrites some with other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n{.*, bar:'apatisk'}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apatisk"))))
  }

  test("projecting with null value") {
    createNode("foo" -> 1, "bar" -> "apa")
    val result = executeWith(Configs.Optional, "OPTIONAL MATCH (n) RETURN n{.*, baz: toString(NULL)}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("projecting from a null identifier produces a null value") {

    val result = executeWith(Configs.Optional, "OPTIONAL MATCH (n) RETURN n{.foo, .bar}")

    result.toList should equal(List(Map("n" -> null)))
  }

  test("projecting with distinct should not drop results") {
    // it turns out that if there is a node with Id=8, then the node with Id=14 will not be seen as distinct if there are other keys in the map (bug in MapValue.equals)
    graph.withTx { tx =>
      tx.execute(
        """
          |UNWIND [8,14] AS cnt
          | CREATE (z:TestNode)
          |SET z.Id=toString(cnt),z.note="testing"
          |""".stripMargin).close()
    }

    Seq(
      (Configs.InterpretedAndSlottedAndPipelined + Configs.Compiled, "MATCH (z:TestNode) RETURN DISTINCT {Id:z.Id, note:z.note} AS x"),
      (Configs.InterpretedAndSlottedAndPipelined, "MATCH (z:TestNode) RETURN DISTINCT z{.Id, .note} AS x")).foreach {
      case (configs: TestConfiguration, query: String) =>
        val result = executeWith(configs, query)

        withClue(query) {
          result.toList should equal(List(
            Map("x" -> Map("note" -> "testing", "Id" -> "8")),
            Map("x" -> Map("note" -> "testing", "Id" -> "14"))
          ))
        }
    }
  }

  test("graph projections with aggregation") {

    val actor = createLabeledNode(Map("name" -> "Actor 1"), "Actor")
    relate(actor, createLabeledNode(Map("title" -> "Movie 1"), "Movie"))
    relate(actor, createLabeledNode(Map("title" -> "Movie 2"), "Movie"))

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, """MATCH (actor:Actor)-->(movie:Movie)
                                                                          |RETURN actor{ .name, movies: collect(movie{.title}) }""".stripMargin)
    result.toList should equal(
      List(Map("actor" ->
        Map("name" -> "Actor 1", "movies" -> Seq(
          Map("title" -> "Movie 1"),
          Map("title" -> "Movie 2"))))))
  }

  test("prepending item to a list should behave correctly in all runtimes") {
    val query = "CYPHER WITH {a:[1,2,3]} AS x RETURN 'a:' + x.a AS r"

    val result = executeWith(Configs.All, query)

    result.toList.head("r") should equal(List("a:", 1, 2, 3))
  }

  test("appending item to a list should behave correctly in all runtimes") {
    val query = "CYPHER WITH {a:[1,2,3]} AS x RETURN x.a + 'a:' AS r"

    val result = executeWith(Configs.All, query)

    result.toList.head("r") should equal(List(1, 2, 3, "a:"))
  }

  test("not(), when right of a =, should give a helpful error message") {
    val query = "RETURN true = not(42 = 32)"

    val config =  Configs.All

    failWithError(config, query,
      List("Unknown function 'not'. If you intended to use the negation expression, surround it with parentheses."))
  }

  test("NOT(), when right of a =, should give a helpful error message") {
    val query = "RETURN true = NOT(42 = 32)"

    // this should have the right error message for 3.1 after the next patch releases
    val config =  Configs.All

    failWithError(config, query,
      List("Unknown function 'NOT'. If you intended to use the negation expression, surround it with parentheses."))
  }

  test("should be able to divide long property") {
    createNode(Map("prop" -> 7909446955L))

    val query = "MATCH (n) RETURN (n.prop / 5) AS div"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("div" -> 1581889391)))
  }

  test("should be able to calculate modulo of long property") {
    createNode(Map("prop" -> 7909446955L))

    val query = "MATCH (n) RETURN (n.prop % 4) AS modulo"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("modulo" -> 3)))
  }

  test("should be able to calculate long property modulo float") {
    createNode(Map("prop" -> 7909446955L))

    val query = "MATCH (n) RETURN (n.prop % 3.5) AS modulo"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("modulo" -> 0.5)))
  }

  test("simple CASE with expression in head") {
    createNode(Map("eyes" -> "blue"))
    createNode(Map("eyes" -> "brown"))
    createNode(Map("eyes" -> "green"))

    val query = """MATCH (n)
                  |RETURN
                  |  CASE n.eyes
                  |    WHEN 'blue'  THEN 1
                  |    WHEN 'brown' THEN 2
                  |    ELSE 3
                  |  END AS result
                  |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should contain theSameElementsAs List(Map("result" -> 1), Map("result" -> 2), Map("result" -> 3))
  }

  test("simple CASE with no expression in head") {
    createNode(Map("eyes" -> "blue"))
    createNode(Map("eyes" -> "brown"))
    createNode(Map("eyes" -> "green"))

    val query = """MATCH (n)
                  |RETURN
                  |  CASE
                  |    WHEN n.eyes = 'blue'  THEN 1
                  |    WHEN n.eyes = 'brown' THEN 2
                  |    ELSE 3
                  |  END AS result
                  |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should contain theSameElementsAs List(Map("result" -> 1), Map("result" -> 2), Map("result" -> 3))
  }

  test("generic CASE with null check") {
    val query = """MATCH (n)
                  |WITH collect(n)[0] AS x
                  |RETURN
                  |  CASE
                  |    WHEN x:L THEN "has-label"
                  |    ELSE "default"
                  |  END AS nullCheck,
                  |  CASE
                  |    WHEN true THEN x:L
                  |    ELSE "default"
                  |  END AS nullResult,
                  |  CASE
                  |    WHEN false THEN "false"
                  |    ELSE x:L
                  |  END AS nullDefault
                  |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should contain theSameElementsAs
      List(Map("nullCheck" -> "default", "nullResult" -> null, "nullDefault" -> null))
  }

  test("compiled expressions should handle null path in nested functions 1") {
    createNode()
    val query =
      """
        |CYPHER runtime=slotted expressionEngine=compiled
        |MATCH (n)
        |OPTIONAL MATCH path=(n)-[*..]->()
        |RETURN collect(relationships(path)) AS value
        |""".stripMargin

    val result = executeSingle(query)
    result.toList should be(List(Map("value" -> List())))
  }

  test("compiled expressions should handle null path in nested functions 2") {
    val query =
      """
        |CYPHER runtime=slotted expressionEngine=compiled
        |OPTIONAL MATCH path=()-[*..]->()
        |WITH path
        |RETURN collect(last(relationships(path))) AS value
        |""".stripMargin

    val result = executeSingle(query)
    result.toList should be(List(Map("value" -> List())))
  }
}
