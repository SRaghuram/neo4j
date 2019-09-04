/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

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

  test("deprecated functions still work") {
    val r = relate(createNode(), createNode())

    executeWith(Configs.InterpretedAndSlotted, "RETURN toInt('1') AS one").columnAs[Long]("one").next should equal(1L)
    executeWith(Configs.InterpretedAndSlotted, "RETURN upper('abc') AS a").columnAs[String]("a").next should equal("ABC")
    executeWith(Configs.InterpretedAndSlotted, "RETURN lower('ABC') AS a").columnAs[String]("a").next should equal("abc")
    executeWith(Configs.InterpretedAndSlotted, "MATCH p = ()-->() RETURN rels(p) AS r").columnAs[List[Relationship]]("r").next should equal(List(r))
  }

  test("node id seek should work with floats") {
    // given
    val idResult = executeSingle("CREATE (n) RETURN id(n) AS id, n").toList.head
    val id = idResult("id")
    val n = idResult("n")

    // when
    val result = executeWith(Configs.InterpretedAndSlotted, s"MATCH (n) WHERE id(n)=${id}.0 RETURN n",
      expectedDifferentResults = Configs.RulePlanner)

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
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost3_1 - Configs.Cost2_3, s"MATCH ()-[r:R]->() WHERE id(r)=${id}.0 RETURN r")

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
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Cost3_1 - Configs.Cost2_3, s"MATCH ()-[r:R]-() WHERE id(r)=${id}.0 RETURN r")

    // then
    result.toList should equal(
      List(Map("r" -> r),
        Map("r" -> r))
    )
  }
}
