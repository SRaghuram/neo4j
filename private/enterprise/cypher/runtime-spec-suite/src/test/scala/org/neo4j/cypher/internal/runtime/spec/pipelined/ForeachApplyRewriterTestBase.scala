/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.QueryPlanTestSupport
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class ForeachApplyRewriterTestBase[CONTEXT <: RuntimeContext](
                                                                     edition: Edition[CONTEXT],
                                                                     runtime: CypherRuntime[CONTEXT],
                                                                     sizeHint: Int
                                                                   ) extends RuntimeTestSuite[CONTEXT](edition, runtime)
                                                                     with QueryPlanTestSupport {
  test("rewrites foreach-apply into limit, apply and unwind") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1, 2, 3]")
      .|.create(createNode("y"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputValues(inputRows: _*))

    executionPlanDescription should includeSomewhere
      .aPlan("SelectOrSemiApply")
      .withLHS(
        aPlan("Projection")
          .onTopOf(aPlan("Input"))
      )
      .withRHS(
        aPlan("ExhaustiveLimit")
        .onTopOf(aPlan("Create")
        .onTopOf(aPlan("Unwind")
        .onTopOf(aPlan("Argument"))))
      )

    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("handle deeply nested foreach") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreachApply("i", "[1]")
      .|.foreachApply("j", "[1, 2]")
      .|.|.foreachApply("k", "[1, 2, 3]")
      .|.|.|.foreachApply("l", "[1, 2, 3, 4]")
      .|.|.|.|.foreachApply("m", "[1, 2, 3, 4, 5]")
      .|.|.|.|.|.create(createNode("y"))
      .|.|.|.|.|.argument("x", "i", "j", "k", "l")
      .|.|.|.|.argument("x", "i", "j", "k")
      .|.|.|.argument("x", "i", "j")
      .|.|.argument("x", "i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build(readOnly = false)

    // then
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputValues(inputRows: _*))
    executionPlanDescription should includeSomewhere
      .aPlan("SelectOrSemiApply")
      .withLHS(aPlan("Projection").onTopOf(aPlan("Input")))
      .withRHS(aPlan("ExhaustiveLimit")
        .onTopOf(aPlan("Apply")
          .withLHS(aPlan("Unwind"))
          .withRHS(aPlan("SelectOrSemiApply")
            .withLHS(aPlan("Projection").onTopOf(aPlan("Argument").withExactVariables("x")))
            .withRHS(aPlan("ExhaustiveLimit")
              .withLHS(aPlan("Apply")
                .withLHS(aPlan("Unwind"))
                .withRHS(aPlan("SelectOrSemiApply")
                  .withLHS(aPlan("Projection").onTopOf(aPlan("Argument").withExactVariables("x", "i")))
                  .withRHS(aPlan("ExhaustiveLimit")
                    .withLHS(aPlan("Apply")
                      .withLHS(aPlan("Unwind"))
                      .withRHS(aPlan("SelectOrSemiApply")
                        .withLHS(aPlan("Projection").onTopOf(aPlan("Argument").withExactVariables("x", "i", "j")))
                        .withRHS(aPlan("ExhaustiveLimit")
                          .withLHS(aPlan("Apply")
                            .withLHS(aPlan("Unwind"))
                            .withRHS(aPlan("SelectOrSemiApply")
                              .withLHS(aPlan("Projection").onTopOf(aPlan("Argument").withExactVariables("x", "i", "j", "k")))
                              .withRHS(aPlan("ExhaustiveLimit")
                                .onTopOf(aPlan("Create")
                                  .onTopOf(aPlan("Unwind")
                                    .onTopOf(aPlan("Argument").withExactVariables("x", "i", "j", "k", "l")))
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )

    runtimeResult should beColumns("x").withRows(inputRows)
  }
}

//+--------------------------------+
//| Operator                       |
//+--------------------------------+
//| +*                             |
//| |                              +
//| +SelectOrSemiApply             |
//| |\                             +
//| | +ExhaustiveLimit             |
//| | |                            +
//| | +Apply                       |
//| | |\                           +
//| | | +SelectOrSemiApply         |
//| | | |\                         +
//| | | | +ExhaustiveLimit         |
//| | | | |                        +
//| | | | +Apply                   |
//| | | | |\                       +
//| | | | | +SelectOrSemiApply     |
//| | | | | |\                     +
//| | | | | | +ExhaustiveLimit     |
//| | | | | | |                    +
//| | | | | | +Apply               |
//| | | | | | |\                   +
//| | | | | | | +SelectOrSemiApply |
//| | | | | | | |\                 +
//| | | | | | | | +ExhaustiveLimit |
//| | | | | | | | |                +
//| | | | | | | | +Apply           |
//| | | | | | | | |\               +
//| | | | | | | | | +Create        |
//| | | | | | | | | |              +
//| | | | | | | | | +Argument      |
//| | | | | | | | |                +
//| | | | | | | | +Unwind          |
//| | | | | | | |                  +
//| | | | | | | +Projection        |
//| | | | | | | |                  +
//| | | | | | | +Argument          |
//| | | | | | |                    +
//| | | | | | +Unwind              |
//| | | | | |                      +
//| | | | | +Projection            |
//| | | | | |                      +
//| | | | | +Argument              |
//| | | | |                        +
//| | | | +Unwind                  |
//| | | |                          +
//| | | +Projection                |
//| | | |                          +
//| | | +Argument                  |
//| | |                            +
//| | +Unwind                      |
//| |                              +
//| +Projection                    |
//| |                              +
//| +Input                         |
//+--------------------------------+