/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, LogicalQueryBuilder}
import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext, MorselRuntime}

object MorselSpecSuite {
  val SIZE_HINT = 1000
}

class MorselSchedulerTracerTest extends SchedulerTracerTestBase(MorselRuntime)

// ALL NODE SCAN
class MorselAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselAllNodeScanStressTest extends AllNodeScanStressTestBase(MorselRuntime)

// INDEX SEEK
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
                              with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class MorselIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(MorselRuntime)
class MorselIndexSeekExactStressTest extends IndexSeekExactStressTest(MorselRuntime)

// LABEL SCAN
class MorselLabelScanTest extends LabelScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselLabelScanStressTest extends LabelScanStressTestBase(MorselRuntime)

// INDEX SCAN
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselIndexScanStressTest extends IndexScanStressTestBase(MorselRuntime)

// INDEX CONTAINS SCAN
class MorselNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselIndexContainsScanStressTest extends IndexContainsScanStressTestBase(MorselRuntime)

// EXPAND
class MorselExpandAllTest extends ExpandAllTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselExpandStressTest extends ExpandStressTestBase(MorselRuntime)

// EAGER AGGREGATION

class MorselAggregationTest extends AggregationTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)

class MorselAggregationStressTest extends ParallelStressSuite(MorselRuntime) /*with RHSOfApplyOneChildStressSuite with RHSOfCartesianOneChildStressSuite*/ with OnTopOfParallelInputStressTest {

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

  // FIXME broken in Morsel right now
//  override def rhsOfApplyOperator(variable: String) =
//    RHSOfApplyOneChildTD(
//      _.aggregation(
//        Map("g" -> modulo(prop("y", "prop"), literalInt(2))),
//        Map("amount" -> sum(add(varFor("prop"), prop("y", "prop"))))),
//      rowsComingIntoTheOperator =>
//        for {
//          (_, rowsForX) <- rowsComingIntoTheOperator.groupBy(_ (0)) // group by x
//          (g, rowsForXAndY) <- rowsForX.groupBy(_ (1).getId.toInt % 2) // group by y.prop % 2
//          amount = rowsForXAndY.map { row =>
//            val Array(x, y) = row
//            x.getId + y.getId
//          }.sum
//        } yield Array(g, amount)
//      ,
//      Seq("g", "amount")
//    )
//
//  override def rhsOfCartesianOperator(variable: String) =
//    RHSOfCartesianOneChildTD(
//      _.aggregation(
//        Map("g" -> modulo(prop("y", "prop"), literalInt(2))),
//        Map("amount" -> sum(prop("y", "prop")))),
//      rowsComingIntoTheOperator =>
//        for {
//          (g, rowsForY) <- rowsComingIntoTheOperator.groupBy(_ (0).getId.toInt % 2) // group by y.prop % 2
//          amount = rowsForY.map { row =>
//            val Array(y) = row
//            y.getId
//          }.sum
//        } yield Array(g, amount)
//      ,
//      Seq("g", "amount")
//    )

  test("should support chained aggregations") {
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

  test("should support aggregations after two expands") {
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

// FILTER

class MorselFilterTest extends FilterTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselFilterStressTest extends FilterStressTestBase(MorselRuntime)

// PROJECTION
class MorselProjectionTest extends ProjectionTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselProjectionStressTest extends ProjectionStressTestBase(MorselRuntime)

// UNWIND
class MorselUnwindTest extends UnwindTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)

class MorselUnwindStressTest extends ParallelStressSuite(MorselRuntime) with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.unwind(s"[$propVariable, 2 * $propVariable] AS i"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          f <- 1 to 2
        } yield Array(x.getId * f),
      Seq("i")
    )
}

// ARGUMENT

// FIXME broken in Morsel
//class MorselArgumentTest extends ArgumentTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT) with MorselSpecSuite

//class MorselArgumentStressTest extends ParallelStressSuite(MorselRuntime) with RHSOfApplyLeafStressSuite {
//  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
//    RHSOfApplyLeafTD(
//      _.projection(s"$nodeArgument AS $variable").|.argument(variable),
//      rowsComingIntoTheOperator =>
//        for {
//          Array(x) <- rowsComingIntoTheOperator
//        } yield Array(x, x)
//    )
//}

// INPUT

class MorselInputTest extends ParallelInputTestBase(MorselRuntime)

// APPLY

class MorselApplyStressTest extends ApplyStressTestBase(MorselRuntime)

// CARTESIAN PRODUCT

class MorselLabelScanCartesianStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with RHSOfCartesianLeafStressSuite {

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.nodeByLabelScan(variable, "Label"),
      () => nodes.map(Array(_))
    )
}

class MorselCartesianProductStressTest extends ParallelStressSuite(MorselRuntime) {

  test("should support nested Cartesian Product") {
    // given
    init()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeIndexOperator("c:Label(prop <= 10)")
      .|.nodeIndexOperator("b:Label(prop <= 20)")
      .nodeIndexOperator("a:Label(prop <= 40)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- nodes if a.getId <= 40
      b <- nodes if b.getId <= 20
      c <- nodes if c.getId <= 10
    } yield Array(a, b, c)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }

  // FIXME broken
//  test("should support Cartesian Product on RHS of apply") {
//    // given
//    init()
//
//    // when
//    val logicalQuery = new LogicalQueryBuilder(this)
//      .produceResults("a", "b", "c")
//      .apply()
//      .|.cartesianProduct()
//      .|.|.nodeIndexOperator("c:Label(prop > ???)", paramExpr = Some(prop("a", "prop")), argumentIds = Set("a"))
//      .|.nodeIndexOperator("b:Label(prop < ???)", paramExpr = Some(prop("a", "prop")), argumentIds = Set("a"))
//      .nodeIndexOperator("a:Label(prop <= 40)")
//      .build()
//
//    val runtimeResult = execute(logicalQuery, runtime)
//
//    // then
//    val expected = for {
//      a <- nodes if a.getId <= 40
//      b <- nodes if b.getId < a.getId
//      c <- nodes if c.getId > a.getId
//    } yield Array(a, b, c)
//    runtimeResult should beColumns("a", "b", "c").withRows(expected)
//  }
}
