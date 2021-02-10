/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder

abstract class CartesianProductStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime) {

  test("should support nested Cartesian Product") {
    // given
    init()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeIndexOperator("c:Label(prop <= 10)")
      .|.nodeIndexOperator("b:Label(prop <= 20)")
      .nodeIndexOperator("a:Label(prop <= 40)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- nodes if a.getId <= 40
      b <- nodes if b.getId <= 20
      c <- nodes if c.getId <= 10
    } yield Array(a, b, c)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }

  test("should support Cartesian Product on RHS of apply") {
    // given
    init()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.cartesianProduct()
      .|.|.nodeIndexOperator("c:Label(prop > ???)", paramExpr = Some(prop("a", "prop")), argumentIds = Set("a"))
      .|.nodeIndexOperator("b:Label(prop < ???)", paramExpr = Some(prop("a", "prop")), argumentIds = Set("a"))
      .nodeIndexOperator("a:Label(prop <= 40)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- nodes if a.getId <= 40
      b <- nodes if b.getId < a.getId
      c <- nodes if c.getId > a.getId
    } yield Array(a, b, c)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }
}
