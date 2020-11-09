/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class NaNAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle NaN comparisons with number correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n.x > 0 AS gt, n.x < 0 AS lt, n.x >= 0 AS ge, n.x <= 0 AS le, n.x = 0 AS eq, n.x <> 0 AS neq")

    // Then
    result.toList should equal(List(Map("gt" -> false, "lt" -> false, "le" -> false, "ge" -> false, "eq" -> false, "neq" -> true)))
  }

  test("should handle NaN predicates correctly") {
    // Given
    createLabeledNode(Map("x" -> Double.NaN), "L")

    // When & Then
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x > 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x >= 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x < 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x <= 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.All, "MATCH (n:L) WHERE n.x = 0.0/0.0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x IS NOT NULL RETURN count(n.x)").toList should be(List(Map("count(n.x)" -> 1)))
    executeWith(Configs.All, "MATCH (n:L) WHERE n.x <> 0.0/0.0 RETURN count(n.x)").toList should be(List(Map("count(n.x)" -> 1)))
  }

  test("should handle NaN index predicates correctly") {
    // Given
    createLabeledNode(Map("x" -> Double.NaN), "L")
    graph.createIndex("L", "x")

    // When & Then
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x > 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x >= 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x < 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x <= 0 RETURN n.x").toList should be(empty)
    executeWith(Configs.All, "MATCH (n:L) WHERE n.x = 0.0/0.0 RETURN n.x").toList should be(empty)
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x IS NOT NULL RETURN count(n.x)").toList should be(List(Map("count(n.x)" -> 1)))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L) WHERE n.x <> 0.0/0.0 RETURN count(n.x)").toList should be(List(Map("count(n.x)" -> 1)))
  }

  test("should handle NaN comparisons with string correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n.x > 'a' AS gt, n.x < 'a' AS lt, n.x >= 'a' AS ge, n.x <= 'a' AS le, n.x = 'a' AS eq, n.x <> 'a' AS neq")

    // Then
    result.toList should equal(List(Map("gt" -> null, "lt" -> null, "le" -> null, "ge" -> null, "eq" -> false, "neq" -> true)))
  }

  test("should handle NaN compared to NaN") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "PROFILE MATCH (n) RETURN n.x > n.x AS gt, n.x < n.x AS lt, n.x <= n.x AS le, n.x >= n.x AS ge, n.x = n.x AS eq, n.x <> n.x AS neq")

    // Then
    result.toList should equal(List(Map("gt" -> false, "lt" -> false, "le" -> false, "ge" -> false, "eq" -> false, "neq" -> true)))
  }

  test("should handle NaN null checks correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n.x IS NULL AS nu")

    // Then
    result.toList should equal(List(Map("nu" -> false)))
  }

  test("should handle NaN null and not null checks correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) RETURN n.x IS NULL AS nu, n.x IS NOT NULL AS nnu")

    // Then
    result.toList should equal(List(Map("nu" -> false, "nnu" -> true)))
  }
}
