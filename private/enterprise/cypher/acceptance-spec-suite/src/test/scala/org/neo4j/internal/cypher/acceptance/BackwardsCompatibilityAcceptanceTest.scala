/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

class BackwardsCompatibilityAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("query without removed syntax should work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN reverse('emil') as backwards")
    result.toList should be(List(Map("backwards" -> "lime")))
  }

  test("toInt should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN toInt('1') AS one")
    result.toList should be(List(Map("one" -> 1)))
  }

  test("upper should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN upper('foo') AS upper")
    result.toList should be(List(Map("upper" -> "FOO")))
  }

  test("lower should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN lower('BAR') AS lower")
    result.toList should be(List(Map("lower" -> "bar")))
  }

  test("rels should still work with CYPHER 3.5") {

    // GIVEN
    val a = createNode(Map("name" -> "Alice"))
    val b = createNode(Map("name" -> "Bob"))
    val c = createNode(Map("name" -> "Charlie"))

    relate(a, b, "prop" -> "ab")
    relate(b, c, "prop" -> "bc")
    relate(a, c, "prop" -> "ac")

    // WHEN
    val query =
      """
        |CYPHER 3.5
        |MATCH p = ({name:'Alice'})-->()
        |UNWIND [r IN rels(p) | r.prop] AS prop
        |RETURN prop ORDER BY prop
      """.stripMargin

    val result = executeSingle(query)

    // THEN
    result.toList should be(List(Map("prop" -> "ab"), Map("prop" -> "ac")))
  }

  test("filter should still work with CYPHER 3.5 regardless of casing") {
    for( filter <- List("filter", "FILTER", "filTeR")) {
      val result = executeSingle(s"CYPHER 3.5 WITH [1,2,3] AS list RETURN $filter(x IN list WHERE x % 2 = 1) AS odds")
      result.toList should be(List(Map("odds" -> List(1, 3))))
    }
  }

  test("extract should still work with CYPHER 3.5 regardless of casing") {
    for( extract <- List("extract", "EXTRACT", "exTraCt")) {
      val result = executeSingle(s"CYPHER 3.5 WITH [1,2,3] AS list RETURN $extract(x IN list | x * 10) AS tens")
      result.toList should be(List(Map("tens" -> List(10, 20, 30))))
    }
  }

  test("old parameter syntax should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN {param} AS answer", params = Map("param" -> 42))
    result.toList should be(List(Map("answer" -> 42)))
  }

  test("length of string should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN length('a string') as len")
    result.toList should be(List(Map("len" -> 8)))
  }

  test("length of collection should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN length([1, 2, 3]) as len")
    result.toList should be(List(Map("len" -> 3)))
  }

  test("length of pattern expression should still work with CYPHER 3.5") {

    // GIVEN
    val a = createNode(Map("name" -> "Alice"))
    val b = createNode(Map("name" -> "Bob"))
    val c = createNode(Map("name" -> "Charlie"))
    val d = createNode(Map("name" -> "David"))

    // a -> b -> c -> d
    relate(a, b)
    relate(b, c)
    relate(c, d)

    // a -> c -> b
    relate(a, c)
    relate(c, b)

    // WHEN
    val result = executeSingle("CYPHER 3.5 MATCH (a) WHERE a.name='Alice' RETURN length((a)-->()-->()) as len")

    // THEN
    result.toList should be(List(Map("len" -> 3))) // a -> b -> c, a -> c -> b, a -> c -> d
  }

  test( "should handle switch between Cypher versions" ) {
    // run query against latest version
    executeSingle("MATCH (n) RETURN n")

    // toInt should work if compatibility mode is set to 3.5
    executeSingle("CYPHER 3.5 RETURN toInt('1') AS one")

    // toInt should fail in latest version
    val exception = the [SyntaxException] thrownBy {
      executeSingle("RETURN toInt('1') AS one")
    }
    exception.getMessage should include("The function toInt() is no longer supported. Please use toInteger() instead")
  }
}
