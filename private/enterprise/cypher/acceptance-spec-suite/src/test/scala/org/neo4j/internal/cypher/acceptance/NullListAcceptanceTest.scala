/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class NullListAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Comparison between lists and non-lists

  test("equality between list and literal should return false") {
    val query = "WITH [1, 2] AS l1, 'foo' AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  // Equality between lists with null

  test("equality of lists of different length should return false despite nulls") {
    val query = "WITH [1] AS l1, [1, null] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between different lists with null should return false") {
    val query = "WITH [1, 2] AS l1, [null, 'foo'] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between almost equal lists with null should return null") {
    val query = "WITH [1, 2] AS l1, [null, 2] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> null)))
  }

  // Nested Lists
  test("equality of nested lists of different length should return false despite nulls") {
    val query = "WITH [[1]] AS l1, [[1], [null]] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between different nested lists with null should return false") {
    val query = "WITH [[1, 2], [1, 3]] AS l1, [[1, 2], [null, 'foo']] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between almost equal nested lists with null should return null") {
    val query = "WITH [[1, 2], ['foo', 'bar']] AS l1, [[1, 2], [null, 'bar']] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> null)))
  }

  // IN with null

  test("IN with different length lists should return false despite nulls") {
    val query = "WITH [1] AS l1, [1, null] AS l2 RETURN l1 IN [l2] AS res"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("IN should return true if match despite nulls") {
    val query = "WITH 3 AS l1, [1, null, 3] AS l2 RETURN l1 IN l2 AS res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> true)))
  }

  test("IN should return null if comparison with null is required") {
    val query = "WITH 4 AS l1, [1, null, 3] AS l2 RETURN l1 IN l2 AS res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> null)))
  }

  // IN with null, list version

  test("IN should return true if correct list found despite other lists having nulls") {
    val query = "WITH [1, 2] AS l1, [[null, 'foo'], [1, 2]] AS l2 RETURN l1 IN l2 AS res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> true)))
  }

  test("IN should return false if no match can be found, despite nulls") {
    val query = "WITH [1,2] AS l1, [[null, 'foo', 'bar']] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("IN should return null if comparison with null is required, list version") {
    val query = "WITH [1,2] AS l1, [[null, 2]] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> null)))
  }

  test("IN should return true with previous null match, list version") {
    val query = "WITH [1,2] AS l1, [[null, 2], [1, 2]] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> true)))
  }

  test("IN should return null if comparison with null is required, list version 2") {
    val query = "WITH [1,2] AS l1, [[null, 2], [1, 3]] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("res" -> null)))
  }
}
