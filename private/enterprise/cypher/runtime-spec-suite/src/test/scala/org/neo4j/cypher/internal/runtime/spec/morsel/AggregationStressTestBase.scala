/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class AggregationStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime) with RHSOfApplyOneChildStressSuite with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.aggregation(
        Map("g" -> modulo(varFor(propVariable), literalInt(2))),
        Map("amount" -> sum(varFor(propVariable)))),
      rowsComingIntoTheOperator =>
        for {
          (g, rowsForX) <- rowsComingIntoTheOperator.groupBy(_ (0).getId.toInt % 2) // group by x.prop % 2
          amount = rowsForX.map { row =>
            val Array(y) = row
            y.getId
          }.sum
        } yield Array(g, amount)
      ,
      Seq("g", "amount")
    )

  override def rhsOfApplyOperator(variable: String) =
    RHSOfApplyOneChildTD(
      _.aggregation(
        Map("g" -> modulo(prop("y", "prop"), literalInt(2))),
        Map("amount" -> sum(add(varFor("prop"), prop("y", "prop"))))),
      rowsComingIntoTheOperator =>
        for {
          (_, rowsForX) <- rowsComingIntoTheOperator.groupBy(_ (0)) // group by x
          (g, rowsForXAndY) <- rowsForX.groupBy(_ (1).getId.toInt % 2) // group by y.prop % 2
          amount = rowsForXAndY.map { row =>
            val Array(x, y) = row
            x.getId + y.getId
          }.sum
        } yield Array(g, amount)
      ,
      Seq("g", "amount")
    )

  ignore("should support chained aggregations") {
    // given
    init()

    val input = allNodesNTimes(10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s")
      .aggregation(Map.empty, Map("s" -> sum(varFor("amount"))))
      .aggregation(
        Map("g" -> modulo(prop("x", "prop"), literalInt(2))),
        Map("amount" -> sum(prop("x", "prop"))))
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedSingleRow = for {
      (_, rows) <- singleNodeInput(input).groupBy(_ (0).getId.toInt % 2) // group by x.prop % 2
      amount = rows.map { row =>
        val Array(x) = row
        x.getId
      }.sum
    } yield amount
    runtimeResult should beColumns("s").withSingleRow(expectedSingleRow.sum)
  }

  ignore("should support aggregations after two expands") {
    // given
    init()

    val input = allNodesNTimes(1)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s")
      .aggregation(Map.empty, Map("s" -> count(varFor("x"))))
      .expand("(next)-[:NEXT]->(secondNext)")
      .expand("(x)-[:NEXT]->(next)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("s").withSingleRow(singleNodeInput(input).size * 5 * 5)
  }
}
