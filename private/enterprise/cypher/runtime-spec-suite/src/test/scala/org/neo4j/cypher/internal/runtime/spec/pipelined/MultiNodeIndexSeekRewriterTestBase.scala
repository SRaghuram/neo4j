/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.QueryPlanTestSupport
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.Parser
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.pipelined.rewriters.combineCartesianProductOfMultipleIndexSeeks.CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.Cardinality

abstract class MultiNodeIndexSeekRewriterTestBase[CONTEXT <: RuntimeContext](
                                                                              edition: Edition[CONTEXT],
                                                                              runtime: CypherRuntime[CONTEXT],
                                                                              sizeHint: Int
                                                                            ) extends RuntimeTestSuite[CONTEXT](edition, runtime)
                                                                              with QueryPlanTestSupport {
  test("preserves order with multiple index seeks - provided order") {
    // given
    val nValues = 14 // gives 819 results in the range 0-12
    val inputRows = inputValues((0 until nValues).map { i =>
      Array[Any](i.toLong, i.toLong)
    }.toArray: _*)

    index("Label", "prop")
    given {
      nodePropertyGraph(nValues, {
        case i: Int => Map("prop" -> (nValues + 3 - i) % nValues) // Reverse and offset when creating the values so we do not accidentally get them in order
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nn", "mm")
      .projection("n.prop as nn", "m.prop as mm")
      .apply()
      .|.cartesianProduct().withLeveragedOrder()
      .|.|.nodeIndexOperator("m:Label(prop < ???)", paramExpr = Some(varFor("j")), getValue = DoNotGetValue)
                            .withCardinalityEstimation(CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD + Cardinality(1))
      .|.nodeIndexOperator("n:Label(prop < ???)",
                           paramExpr = Some(varFor("i")),
                           getValue = GetValue)
                          .withCardinalityEstimation(CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD + Cardinality(1))
                          .withProvidedOrder(ProvidedOrder.asc(Parser.parseExpression("n.prop")))
      .input(variables = Seq("i", "j"))
      .build()

    // then
    val expected = for {i <- 0 until nValues
                        j <- 0 until i
                        k <- 0 until i} yield Array(j, k)
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputRows)

    executionPlanDescription should includeSomewhere.nTimes(1, aPlan("MultiNodeIndexSeek"))
    executionPlanDescription shouldNot includeSomewhere.aPlan("NodeIndexSeekByRange")
    executionPlanDescription shouldNot includeSomewhere.aPlan("CartesianProduct")

    runtimeResult should beColumns("nn", "mm").withRows(inOrder(expected))
  }

  test("preserves order with multiple index seeks - low lhs cardinality") {
    // given
    val nValues = 14 // gives 819 results in the range 0-12
    val inputRows = inputValues((0 until nValues).map { i =>
      Array[Any](i.toLong, i.toLong)
    }.toArray: _*)

    index("Label", "prop")
    given {
      nodePropertyGraph(nValues, {
        case i: Int => Map("prop" -> (nValues + 3 - i) % nValues) // Reverse and offset when creating the values so we do not accidentally get them in order
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nn", "mm")
      .projection("n.prop as nn", "m.prop as mm")
      .apply()
      .|.cartesianProduct()
      .|.|.nodeIndexOperator("m:Label(prop < ???)", paramExpr = Some(varFor("j")), getValue = DoNotGetValue)
                            .withCardinalityEstimation(CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD + Cardinality(1))
      .|.nodeIndexOperator("n:Label(prop < ???)",
                           paramExpr = Some(varFor("i")),
                           getValue = GetValue)
                          .withCardinalityEstimation(CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD + Cardinality(-1)) // One below threshold and no leveraged order
      .input(variables = Seq("i", "j"))
      .build()

    // then
    val expected = for {i <- 0 until nValues
                        j <- 0 until i
                        k <- 0 until i} yield Array(j, k)
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputRows)

    executionPlanDescription should includeSomewhere.nTimes(1, aPlan("MultiNodeIndexSeek"))
    executionPlanDescription shouldNot includeSomewhere.aPlan("NodeIndexSeekByRange")
    executionPlanDescription shouldNot includeSomewhere.aPlan("CartesianProduct")

    runtimeResult should beColumns("nn", "mm").withRows(inOrder(expected))
  }

  test("does not rewrite with high lhs cardinality") {
    // given
    val nValues = 14 // gives 819 results in the range 0-12
    val inputRows = inputValues((0 until nValues).map { i =>
      Array[Any](i.toLong, i.toLong)
    }.toArray: _*)

    index("Label", "prop")
    given {
      nodePropertyGraph(nValues, {
        case i: Int => Map("prop" -> (nValues + 3 - i) % nValues) // Reverse and offset when creating the values so we do not accidentally get them in order
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nn", "mm")
      .projection("n.prop as nn", "m.prop as mm")
      .apply()
      .|.cartesianProduct()
      .|.|.nodeIndexOperator("m:Label(prop < ???)", paramExpr = Some(varFor("j")), getValue = DoNotGetValue)
                            .withCardinalityEstimation(CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD + Cardinality(-1)) // Low rhs cardinality
      .|.nodeIndexOperator("n:Label(prop < ???)",
                           paramExpr = Some(varFor("i")),
                           getValue = GetValue)
                          .withCardinalityEstimation(CARTESIAN_PRODUCT_CARDINALITY_THRESHOLD) // At the threshold and no leveraged order
      .input(variables = Seq("i", "j"))
      .build()

    // then
    val expected = for {i <- 0 until nValues
                        j <- 0 until i
                        k <- 0 until i} yield Array(j, k)
    val (runtimeResult, executionPlanDescription) = executeAndExplain(logicalQuery, runtime, inputRows)

    executionPlanDescription shouldNot includeSomewhere.aPlan("MultiNodeIndexSeek")
    executionPlanDescription should includeSomewhere.nTimes(2, aPlan("NodeIndexSeekByRange"))
    executionPlanDescription should includeSomewhere.aPlan("CartesianProduct")

    runtimeResult should beColumns("nn", "mm").withRows(inAnyOrder(expected))
  }
}