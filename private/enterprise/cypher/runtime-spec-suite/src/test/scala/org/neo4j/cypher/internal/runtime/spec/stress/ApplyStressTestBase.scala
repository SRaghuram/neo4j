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

abstract class ApplyStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime) {

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
      .nodeIndexOperator("a:Label(prop <= 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- nodes if a.getProperty("prop").asInstanceOf[Int] <= 20
      b <- nodes if b.getId < a.getId
      c <- nodes if c.getId < b.getId
    } yield Array(a, b, c)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }
}
