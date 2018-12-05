/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
    val result = executeWith(Configs.All + Configs.Morsel, "MATCH (n) RETURN id(n)")

    // THEN
    result.toList should equal(List(Map("id(n)" -> expected)))
  }

  test("id on a rel should work in both runtimes")  {
    // GIVEN
    val expected = relate(createNode(), createNode()).getId

    // WHEN
    val result = executeWith(Configs.All + Configs.Morsel, "MATCH ()-[r]->() RETURN id(r)")

    // THEN
    result.toList should equal(List(Map("id(r)" -> expected)))
  }

  test("deprecated functions still work") {
    val r = relate(createNode(), createNode())

    executeWith(Configs.InterpretedAndSlottedAndMorsel, "RETURN toInt('1') AS one").columnAs[Long]("one").next should equal(1L)
    executeWith(Configs.InterpretedAndSlottedAndMorsel, "RETURN upper('abc') AS a").columnAs[String]("a").next should equal("ABC")
    executeWith(Configs.InterpretedAndSlottedAndMorsel, "RETURN lower('ABC') AS a").columnAs[String]("a").next should equal("abc")
    executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH p = ()-->() RETURN rels(p) AS r").columnAs[List[Relationship]]("r").next should equal(List(r))
  }
}
