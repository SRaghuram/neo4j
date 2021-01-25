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

abstract class AggregationStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime) with RHSOfApplyOneChildStressSuite with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.aggregation(
        Seq(s"$propVariable % 2 AS g"),
        Seq(s"sum($propVariable) AS amount")),
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

  override def rhsOfApplyOperator(variable: String): RHSOfApplyOneChildTD =
    RHSOfApplyOneChildTD(
      _.aggregation(
        Seq("y.prop % 2 AS g"),
        Seq("sum(prop + y.prop) AS amount")),
      rowsComingIntoTheOperator =>
        for {
          (_, rowsForX) <- rowsComingIntoTheOperator.groupBy(_ (0)) // group by x
          (g, rowsForXAndY) <- rowsForX.groupBy(_ (1).getId.toInt % 2) // group by y.prop % 2
          amount = rowsForXAndY.map { row =>
            val Array(x, y) = row
            x.getProperty("propWithDuplicates").asInstanceOf[Int] + y.getId
          }.sum
        } yield Array(g, amount)
      ,
      Seq("g", "amount")
    )

  test("should support chained aggregations") {
    // given
    init()

    val input = allNodesNTimes(10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s")
      .aggregation(Seq.empty, Seq("sum(amount) AS s"))
      .aggregation(
        Seq("x.prop % 2 AS g"),
        Seq("sum(x.prop) AS amount"))
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

  test("should support aggregations after two expands") {
    // given
    init()

    val input = allNodesNTimes(1)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s")
      .aggregation(Seq.empty, Seq("count(x) AS s"))
      .expand("(next)-[:NEXT]->(secondNext)")
      .expand("(x)-[:NEXT]->(next)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("s").withSingleRow(singleNodeInput(input).size * 5 * 5)
  }
}
