/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.Neo4jException

import scala.util.matching.Regex

class SyntaxExceptionAcceptanceTest extends ExecutionEngineFunSuite {

  // Not TCK material; shortestPath

  test("shortest path can not have minimum depth different from zero or one") {
    test(
      "match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath((a)-[*2..3]->(b)) return p",
      "shortestPath(...) does not support a minimal length different from 0 or 1 (line 1, column 54)"
    )
  }

  test("shortest path can not have multiple links in it") {
    test(
      "match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath((a)-->()-->(b)) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 54)"
    )
  }

  test("start is not supported since a long time") {
    test(
      "start a=(0) return a",
      " " // any error message is OK
    )
  }

  // pure syntax errors; are these TCK material?

  test("should raise error when missing return columns") {
    test(
      "match (s) return",
      "Invalid input '': expected \"GRAPH\", \"*\" or an expression (line 1, column 17 (offset: 16))"
    )
  }

  test("should raise error when missing return") {
    test(
      "match (s) where s.id = 0",
      "Query cannot conclude with MATCH (must be RETURN or an update clause) (line 1, column 1)"
    )
  }

  test("forget by in order by") {
    test(
      "match (a) where id(a) = 0 return a order a.name",
      ".*line 1, column 42.*".r
    )
  }

  test("should handle empty string") {
    test(" ", "Unexpected end of input: expected whitespace, comment, CYPHER options, EXPLAIN, PROFILE or Query (line 1, column 2 (offset: 1))")
  }

  test("should handle newline") {
    val sep = System.lineSeparator()
    test(sep, s"Unexpected end of input: expected whitespace, comment, CYPHER options, EXPLAIN, PROFILE or Query (line 2, column 1 (offset: ${sep.length}))")
  }

  def test(query: String, message: String) {
    try {
      execute(query)
      fail(s"Did not get the expected syntax error, expected: $message")
    } catch {
      case x: Neo4jException =>
        val actual = x.getMessage.linesIterator.next.trim
        actual should startWith(message.init)
    }
  }

  def test(query: String, messageRegex: Regex) {
    try {
      execute(query)
      fail(s"Did not get the expected syntax error, expected matching: '$messageRegex'")
    } catch {
      case x: Neo4jException =>
        val actual = x.getMessage.linesIterator.next().trim
        messageRegex findFirstIn actual match {
          case None => fail(s"Expected matching '$messageRegex', but was '$actual'")
          case Some(_) => ()
        }
    }
  }
}
