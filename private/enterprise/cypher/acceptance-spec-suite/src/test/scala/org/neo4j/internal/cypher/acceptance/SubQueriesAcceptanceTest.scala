/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class SubQueriesAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("CALL around union query - using returned var in outer query") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN x"

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val expected = List(Map("x" -> 1), Map("x" -> 2))

    result.toList should equal(expected)
  }
  test("CALL around union query with different return column orders - using returned vars in outer query") {
    val query = "CALL { RETURN 1 as x, 2 AS y UNION RETURN 3 AS y, 2 as x } RETURN x, y"

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val expected = List(Map("x" -> 1, "y" -> 2), Map("x" -> 2, "y" -> 3))

    result.toList should equal(expected)
  }

}
