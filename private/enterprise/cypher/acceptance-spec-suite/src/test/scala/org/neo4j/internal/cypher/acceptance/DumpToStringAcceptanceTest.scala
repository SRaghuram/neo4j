/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class DumpToStringAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  implicit val windowsSafe = WindowsStringSafe

  test("basic dumpToString") {
    dumpToString(
      """UNWIND [
        |  {a:1,                b:true },
        |  {a:'Hello there...', b:5.467},
        |  {a:[1,2],            b:'Hi!'}
        |  ] AS map
        |RETURN map.a AS a, map.b AS bColumn""".stripMargin) should
      equal("""+----------------------------+
              || a                | bColumn |
              |+----------------------------+
              || 1                | true    |
              || "Hello there..." | 5.467   |
              || [1,2]            | "Hi!"   |
              |+----------------------------+
              |3 rows
              |""".stripMargin)
  }

  test("format node") {
    createNode(Map("prop" -> "A"))

    dumpToString("match (n) return n, 2 AS int") should
      equal("""+-------------------------+
              || n                 | int |
              |+-------------------------+
              || Node[0]{prop:"A"} | 2   |
              |+-------------------------+
              |1 row
              |""".stripMargin)
  }

  test("format relationship") {
    relate(createNode(), createNode(), "T", Map("prop" -> "A"))

    dumpToString("match ()-[r]->() return r") should
      equal("""+-----------------+
              || r               |
              |+-----------------+
              || :T[0]{prop:"A"} |
              |+-----------------+
              |1 row
              |""".stripMargin)

  }

  test("format collection of maps") {
    dumpToString( """RETURN [{ inner: 'Map1' }, { inner: 'Map2' }]""") should
      equal(
        """+----------------------------------------+
          || [{ inner: 'Map1' }, { inner: 'Map2' }] |
          |+----------------------------------------+
          || [{inner -> "Map1"},{inner -> "Map2"}]  |
          |+----------------------------------------+
          |1 row
          |""".stripMargin)
  }
}
