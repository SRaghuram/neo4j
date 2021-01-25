/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class IdAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("id on a node should work in both runtimes")  {
    // GIVEN
    val expected = createNode().getId

    // WHEN
    val result = executeWith(Configs.All, "MATCH (n) RETURN id(n)")

    // THEN
    result.toList should equal(List(Map("id(n)" -> expected)))
  }

  test("id on a rel should work in both runtimes")  {
    // GIVEN
    val expected = relate(createNode(), createNode()).getId

    // WHEN
    val result = executeWith(Configs.All, "MATCH ()-[r]->() RETURN id(r)")

    // THEN
    result.toList should equal(List(Map("id(r)" -> expected)))
  }

  test("node id seek should work with floats") {
    // given
    val idResult = executeSingle("CREATE (n) RETURN id(n) AS id, n").toList.head
    val id = idResult("id")
    val n = idResult("n")

    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, s"MATCH (n) WHERE id(n)=$id.0 RETURN n")

    // then
    result.toList should equal(
      List(Map("n" -> n))
    )
  }

  test("directed rel id seek should work with floats") {
    // given
    val idResult = executeSingle("CREATE ()-[r:R]->() RETURN id(r) AS id, r").toList.head
    val id = idResult("id")
    val r = idResult("r")

    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined - Configs.Parallel, s"MATCH ()-[r:R]->() WHERE id(r)=$id.0 RETURN r")

    // then
    result.toList should equal(
      List(Map("r" -> r))
    )
  }

  test("undirected rel id seek should work with floats") {
    // given
    val idResult = executeSingle("CREATE ()-[r:R]->() RETURN id(r) AS id, r").toList.head
    val id = idResult("id")
    val r = idResult("r")

    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined - Configs.Parallel, s"MATCH ()-[r:R]-() WHERE id(r)=$id.0 RETURN r")

    // then
    result.toList should equal(
      List(Map("r" -> r),
        Map("r" -> r))
    )
  }
}
