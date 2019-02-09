/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.ZombieRuntime
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_EDITION, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.v4_0.logical.plans.Descending

class ZombieTestSuite extends RuntimeTestSuite(ENTERPRISE_EDITION, ZombieRuntime) {

  test("should handle expand") {
    // given
    val (nodes, rels) = circleGraph(10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getStartNode, r.getEndNode),
                    Array(r.getEndNode, r.getStartNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should apply-sort") {
    // given
    val (nodes, rels) = circleGraph(10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort(Descending("y"))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(groupedBy("x").desc("y"))
  }
}
