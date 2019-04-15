/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class ApplyStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime) {

  test("should support nested Apply") {
    // given
    init()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.apply()
      .|.|.nodeIndexOperator("c:Label(prop < ???)", paramExpr = Some(prop("b", "prop")), argumentIds = Set("a", "b"))
      .|.nodeIndexOperator("b:Label(prop < ???)", paramExpr = Some(prop("a", "prop")), argumentIds = Set("a"))
      .nodeIndexOperator("a:Label(prop <= 40)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- nodes if a.getId <= 40
      b <- nodes if b.getId < a.getId
      c <- nodes if c.getId < b.getId
    } yield Array(a, b, c)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }
}
