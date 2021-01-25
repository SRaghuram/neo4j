/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ReverseAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("reverse function should work on strings") {
    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN reverse('raksO')").columnAs("reverse('raksO')").next().toString

    // Then
    result should equal("Oskar")
  }

  test("reverse function should work with collections of integers") {
    // When
    inTx { tx =>
      val result = tx.execute("with [4923,489,521,487] as ids RETURN reverse(ids)")

      val results = result.columnAs("reverse(ids)").next().toString

      // Then
      results should equal("[487, 521, 489, 4923]")
    }
  }

  test("reverse function should work with collections that contains null") {
    inTx { tx =>
      // When
      val result = tx.execute("with [4923,null,521,487] as ids RETURN reverse(ids)")

      val results = result.columnAs("reverse(ids)").next().toString

      // Then
      results should equal("[487, 521, null, 4923]")
    }
  }

  test("reverse function should work with empty collections") {
    inTx { tx =>
      // When
      val result = tx.execute("with [] as ids RETURN reverse(ids)")

      val results = result.columnAs("reverse(ids)").next().toString

      // Then
      results should equal("[]")
    }
  }

  test("reverse function should work with collections of mixed types") {
    inTx { tx =>
      // When
      val result = tx.execute("with [4923,'abc',521,487] as ids RETURN reverse(ids)")

      val results = result.columnAs("reverse(ids)").next().toString

      // Then
      results should equal("[487, 521, abc, 4923]")
    }
  }

  test("reverse should be able to concatenate to original list") {
    inTx { tx =>
      // When
      val query =
        """
          | WITH range(1, 2) AS xs
          | RETURN xs + reverse(xs) AS res
          | """.stripMargin

      val results = tx.execute(query).columnAs("res").next().toString

      // Then
      results should equal("[1, 2, 2, 1]")
    }
  }
}
