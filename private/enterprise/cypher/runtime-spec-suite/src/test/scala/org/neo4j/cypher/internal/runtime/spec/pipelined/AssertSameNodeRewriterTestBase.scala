/*
 * Copyright (c) "Neo4j"
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

abstract class AssertSameNodeRewriterTestBase[CONTEXT <: RuntimeContext](
                                                                     edition: Edition[CONTEXT],
                                                                     runtime: CypherRuntime[CONTEXT],
                                                                   ) extends RuntimeTestSuite[CONTEXT](edition, runtime)
                                                                     with QueryPlanTestSupport {

  test("should rewrite assertSameNode") {
    //given
    given {
      uniqueIndex("A", "prop")
      uniqueIndex("B", "prop")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:B(prop = 1)")
      .nodeIndexOperator("x:A(prop = 1)")
      .build()

    // when
    val (_, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, input = NO_INPUT)

    //then
    executionPlanDescription should includeSomewhere
      .aPlan("AssertingMultiNodeIndexSeek")
      .containingArgument("x:A(prop) WHERE prop = 1, x:B(prop) WHERE prop = 1")
  }

  test("should rewrite many assertSameNodes") {
    //given
    given {
      uniqueIndex("A", "prop")
      uniqueIndex("B", "prop")
      uniqueIndex("C", "prop")
      uniqueIndex("D", "prop")
      uniqueIndex("E", "prop")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.assertSameNode("x")
      .|.|.assertSameNode("x")
      .|.|.|.assertSameNode("x")
      .|.|.|.|.nodeIndexOperator("x:E(prop = 1)")
      .|.|.|.nodeIndexOperator("x:D(prop = 1)")
      .|.|.nodeIndexOperator("x:C(prop = 1)")
      .|.nodeIndexOperator("x:B(prop = 1)")
      .nodeIndexOperator("x:A(prop = 1)")
      .build()

    // when
    val (_, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, input = NO_INPUT)

    //then
    executionPlanDescription should includeSomewhere
      .aPlan("AssertingMultiNodeIndexSeek")
      .containingArgument("x:A(prop) WHERE prop = 1, x:B(prop) WHERE prop = 1, x:C(prop) WHERE prop = 1, x:D(prop) WHERE prop = 1, x:E(prop) WHERE prop = 1")
  }

  test("should rewrite assertSameNode on the RHS of an APPLY") {
    //given
    given {
      uniqueIndex("A", "prop")
      uniqueIndex("B", "prop")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.assertSameNode("x")
      .|.|.nodeIndexOperator("x:B(prop = 1)")
      .|.nodeIndexOperator("x:A(prop = 1)")
      .argument()
      .build()

    // when
    val (_, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, input = NO_INPUT)

    //then
    executionPlanDescription should includeSomewhere
      .aPlan("AssertingMultiNodeIndexSeek")
      .containingArgument("x:A(prop) WHERE prop = 1, x:B(prop) WHERE prop = 1")
  }
}