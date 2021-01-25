/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder

abstract class ExpressionStressTestBase(edition: Edition[EnterpriseRuntimeContext],
                                        runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime) {

  test("should support getDegree on top of (parallel) all-node scan") {
    // given
    init()
    val aggregationGroups = graphSize / 2

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("k", "c")
      .aggregation(groupingExpressions = Seq(s"id(x) % $aggregationGroups AS k"), aggregationExpression = Seq("count(*) AS c"))
      .filterExpression(greaterThan(getDegree(varFor("x"), SemanticDirection.BOTH), literalInt(10000)))
      .allNodeScan("x")
      .build()


    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("k", "c").withNoRows()
  }
}
