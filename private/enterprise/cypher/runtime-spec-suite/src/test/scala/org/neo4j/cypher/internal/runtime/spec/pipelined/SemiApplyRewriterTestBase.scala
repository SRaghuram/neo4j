/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.QueryPlanTestSupport
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class SemiApplyRewriterTestBase[CONTEXT <: RuntimeContext](
                                                                     edition: Edition[CONTEXT],
                                                                     runtime: CypherRuntime[CONTEXT],
                                                                     sizeHint: Int
                                                                   ) extends RuntimeTestSuite[CONTEXT](edition, runtime)
                                                                     with QueryPlanTestSupport {
  test("rewrites semi-apply into limit and apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .semiApply()
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputValues(inputRows: _*))

    executionPlanDescription should includeSomewhere
      .aPlan("Apply")
        .withLHS(aPlan("Input"))
        .withRHS(aPlan("Limit").onTopOf(aPlan("Argument")))

    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("rewrites select-or-semi-apply into limit and select-or-semi-apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrSemiApply("TRUE")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputValues(inputRows: _*))

    executionPlanDescription should includeSomewhere
      .aPlan("SelectOrSemiApply")
      .withLHS(aPlan("Input"))
      .withRHS(aPlan("Limit").onTopOf(aPlan("Argument")))

    runtimeResult should beColumns("x").withRows(inputRows)
  }
}