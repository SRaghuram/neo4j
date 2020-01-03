/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate
import java.util

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}
import org.neo4j.internal.helpers.collection.MapUtil

class UnwindAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should unwind scalar integer") {
    val query = "UNWIND 7 AS x RETURN x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("x" -> 7)))
  }

  test("should unwind scalar string") {
    val query = "UNWIND 'this string' AS x RETURN x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("x" -> "this string")))
  }

  test("should unwind scalar boolean") {
    val query = "UNWIND false AS x RETURN x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("x" -> false)))
  }

  test("should unwind scalar temporal") {
    val query = "UNWIND date('2019-05-07') AS x RETURN x"
    val result = executeWith(Configs.UDF, query)

    result.toList should equal(List(Map("x" -> LocalDate.of(2019, 5, 7))))
  }

  test("should unwind parameterized scalar value") {
    val query = "UNWIND $value AS x RETURN x"
    val result = executeWith(Configs.All, query, params = Map("value" -> 42))

    result.toList should equal(List(Map("x" -> 42)))

  }

  test("should unwind nodes") {
    val n = createNode("prop" -> 42)

    graph.withTx( tx => {
      val query = "UNWIND $nodes AS n WITH n WHERE n.prop = 42 RETURN n"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("nodes", util.Arrays.asList(n))))
      result.toList should equal(List(Map("n" -> n)))
    })

  }

  test("should unwind nodes from literal list") {
    val n = createNode("prop" -> 42)

    graph.withTx( tx => {
      val query = "UNWIND [$node] AS n WITH n WHERE n.prop = 42 RETURN n"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("node", n)))
      result.toList should equal(List(Map("n" -> n)))
    } )
  }

  test("should unwind scalar node") {

    val n = createNode("prop" -> 42)

    val query =
      """
        | MATCH (n {prop: 42})
        | WITH n
        | UNWIND n AS x
        | RETURN x
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("x" -> n)))
  }

  test("should unwind relationships") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "prop" -> 42)

    graph.withTx( tx => {
      val query = "UNWIND $relationships AS r WITH r WHERE r.prop = 42 RETURN r"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("relationships", util.Arrays.asList(r))))
      result.toList should equal(List(Map("r" -> r)))
    } )
  }

  test("should unwind relationships from literal list") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "prop" -> 42)

    graph.withTx( tx => {
      val query = "UNWIND [$relationship] AS r WITH r WHERE r.prop = 42 RETURN r"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("relationship", r)))
      result.toList should equal(List(Map("r" -> r)))
    } )
  }

  test("should unwind scalar relationship") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val query =
      """
        | MATCH ()-[r]->()
        | WITH r
        | UNWIND r AS x
        | RETURN x
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("x" -> r)))
  }
}
