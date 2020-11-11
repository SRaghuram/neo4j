/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

// Only put tests that assert on memory performance behaviour in this class
class MemoryPerformanceAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("check for contents of collection that contains only a single null") {
    createNode()

    val result = executeWith(Configs.All,
      "MATCH (a) WHERE 42 IN [a.prop] RETURN *", params = Map("param" -> null)
    )

    result shouldBe empty
  }

  test("should be able to handle a large DNF predicate without running out of memory") {
    // given
    val query = """MATCH (a)-[r]->(b) WHERE
                  |(ID(a)= 12466 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12462 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12458 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12447 AND type(r)= 'class2') OR
                  |(ID(a)= 12466 AND ID(b)= 12459 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12464 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12460 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12446 AND type(r)= 'class3') OR
                  |(ID(a)= 12466 AND ID(b)= 12472 AND type(r)= 'class4') OR
                  |(ID(a)= 12466 AND ID(b)= 12457 AND type(r)= 'class1') OR
                  |(ID(a)= 12467 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12467 AND ID(b)= 12459 AND type(r)= 'class1') OR
                  |(ID(a)= 12467 AND ID(b)= 12451 AND type(r)= 'class2') OR
                  |(ID(a)= 12467 AND ID(b)= 12470 AND type(r)= 'class4') OR
                  |(ID(a)= 12467 AND ID(b)= 12445 AND type(r)= 'class3') OR
                  |(ID(a)= 12471 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12471 AND ID(b)= 12467 AND type(r)= 'class4') OR
                  |(ID(a)= 12471 AND ID(b)= 12455 AND type(r)= 'class1') OR
                  |(ID(a)= 12471 AND ID(b)= 12459 AND type(r)= 'class1') OR
                  |(ID(a)= 12471 AND ID(b)= 12452 AND type(r)= 'class2') OR
                  |(ID(a)= 12471 AND ID(b)= 12451 AND type(r)= 'class3') OR
                  |(ID(a)= 12469 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12469 AND ID(b)= 12450 AND type(r)= 'class2') OR
                  |(ID(a)= 12469 AND ID(b)= 12447 AND type(r)= 'class3')
                  |RETURN ID(a), ID(b), type(r)""".stripMargin

    // when
    executeWith(Configs.InterpretedAndSlottedAndPipelined - Configs.Parallel, query)
    // then it should not fail or run out of memory
  }

  test("should unwind a long range without going OOM") {
    val expectedResult = 20000000

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, s"UNWIND range(1, $expectedResult) AS i RETURN count(*) AS c")
    result.columnAs[Long]("c").toList should equal(List(expectedResult))
  }

  test("should be able to do ORDER BY with huge LIMIT") {
    val query = """
                  |WITH [4, 3, 1, 2] AS lst
                  |UNWIND lst AS x
                  |WITH x
                  |ORDER BY x ASC LIMIT 2147483647
                  |RETURN x""".stripMargin

    executeWith(Configs.All, query).toList should equal(List(
      Map("x" -> 1),
      Map("x" -> 2),
      Map("x" -> 3),
      Map("x" -> 4)
    ))
  }
}
