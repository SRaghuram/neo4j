/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.QueryPlanTestSupport
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.pipelined.rewriters.pipelinedPrePhysicalPlanRewriter
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class RollupApplyRewriterTestBase[CONTEXT <: RuntimeContext](
                                                                     edition: Edition[CONTEXT],
                                                                     runtime: CypherRuntime[CONTEXT],
                                                                     sizeHint: Int
                                                                   ) extends RuntimeTestSuite[CONTEXT](edition, runtime)
                                                                     with QueryPlanTestSupport {
  test("should not rewrite roll up apply if under an nested plan expression") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1")
      .nestedPlanCollectExpressionProjection("x1", "b.prop")
      .|.expand("(a)-->(b)")
      .|.rollUpApply("list", "y")
      .|.|.unwind("[1,2,3] AS y")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //when
    val rewrittenPlan = pipelinedPrePhysicalPlanRewriter(logicalQuery, parallelExecution = false)

    //then
    logicalQuery.logicalPlan shouldEqual (rewrittenPlan)
  }

  test("should rewrite roll up apply if child-plan contains a nested plan expression") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("list2")
      .rollUpApply("list2", "y")
      .|.unwind("list1 as y")
      .|.nestedPlanCollectExpressionProjection("list1", "b.prop")
      .|.|.expand("(a)-->(b)")
      .|.|.argument("a")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    //when
    val rewrittenPlan = pipelinedPrePhysicalPlanRewriter(logicalQuery, parallelExecution = false)

    //then
    logicalQuery.logicalPlan shouldNot equal (rewrittenPlan)
  }

  test("should rewrite roll up apply") {
    //given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "list")
      .rollUpApply("list", "y")
      .|.unwind("[1,2,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // when
    val (_, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, input = NO_INPUT)

    //then
    executionPlanDescription should includeSomewhere
      .aPlan("Apply")
      .withRHS(aPlan("EagerAggregation").onTopOf(aPlan("Unwind")))
      .withLHS(aPlan("Input"))
  }
}