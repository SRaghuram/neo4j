/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle map projection with property selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n) RETURN n{.foo,.bar,.baz}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "baz" -> null))
  }

  test("should handle map projection with property selectors and identifier selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "WITH 42 as x MATCH (n) RETURN n{.foo,.bar,x}")

    result.toList.head("n") should equal(Map("foo" -> 1, "bar" -> "apa", "x" -> 42))
  }

  test("should use the map identifier as the alias for return items") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n) RETURN n{.foo,.bar}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection with all-properties selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n) RETURN n{.*}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("returning all properties of a node and adds other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n) RETURN n{.*, .baz}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("returning all properties of a node and overwrites some with other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n) RETURN n{.*, bar:'apatisk'}")

    result.toList should equal(List(Map("n" -> Map("foo" -> 1, "bar" -> "apatisk"))))
  }

  test("projecting from a null identifier produces a null value") {

    val result = executeWith(Configs.Optional, "OPTIONAL MATCH (n) RETURN n{.foo, .bar}")

    result.toList should equal(List(Map("n" -> null)))
  }

  test("graph projections with aggregation") {

    val actor = createLabeledNode(Map("name" -> "Actor 1"), "Actor")
    relate(actor, createLabeledNode(Map("title" -> "Movie 1"), "Movie"))
    relate(actor, createLabeledNode(Map("title" -> "Movie 2"), "Movie"))

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, """MATCH (actor:Actor)-->(movie:Movie)
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
}
