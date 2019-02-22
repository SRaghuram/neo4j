/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_PARALLEL.HasEvidenceOfParallelism
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.{AggregationTestBase, AllNodeScanTestBase, ExpandAllTestBase, FilterTestBase, InputTestBase, LabelScanTestBase, NodeIndexContainsScanTestBase, NodeIndexScanTestBase, NodeIndexSeekRangeAndCompositeTestBase, NodeIndexSeekTestBase, ProjectionTestBase, UnwindTestBase}
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_PARALLEL, LogicalQueryBuilder}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, MorselRuntime}

object MorselSpecSuite {
  val SIZE_HINT = 10000
}

// ALL NODE SCAN
class MorselAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselAllNodeScanStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite with RHSOfCartesianLeafStressSuite {
  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.allNodeScan(variable),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes
        } yield Array(x, y)
    )

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.allNodeScan(variable),
      () => nodes.map(Array(_))
    )
}

// INDEX SEEK
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)
                              with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class MorselIndexSeekRangeStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite with RHSOfCartesianLeafStressSuite {
  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop > ???)", paramExpr = Some(varFor(propArgument)), argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes.filter(_.getProperty("prop").asInstanceOf[Int] > x.getId)
        } yield Array(x, y)
    )

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop > 10)"),
      () => nodes.filter(_.getProperty("prop").asInstanceOf[Int] > 10).map(Array(_))
    )
}

class MorselIndexSeekExactStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite with RHSOfCartesianLeafStressSuite {
  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop = ???)", paramExpr = Some(varFor(propArgument)), argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes.filter(_.getProperty("prop").asInstanceOf[Int] == x.getId)
        } yield Array(x, y)
    )

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop = 10)"),
      () => nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 10).map(Array(_))
    )
}

// LABEL SCAN
class MorselLabelScanTest extends LabelScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselLabelScanStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite with RHSOfCartesianLeafStressSuite {
  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeByLabelScan(variable, "Label"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes
        } yield Array(x, y)
    )

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.nodeByLabelScan(variable, "Label"),
      () => nodes.map(Array(_))
    )

}

// INDEX SCAN
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselIndexScanStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite with RHSOfCartesianLeafStressSuite {
  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop)", argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes
        } yield Array(x, y)
    )

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop)"),
      () => nodes.map(Array(_))
    )
}

// INDEX CONTAINS SCAN
class MorselNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselIndexContainsScanStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite {
  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(text CONTAINS ???)", paramExpr = Some(function("toString", varFor(propArgument))), argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes.filter(_.getProperty("text").asInstanceOf[String].contains(x.getId.toString))
        } yield Array(x, y)
    )
}

// EXPAND
class MorselExpandAllTest extends ExpandAllTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselExpandStressTest extends ParallelStressSuite with RHSOfApplyOneChildStressSuite with RHSOfCartesianOneChildStressSuite with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.expand(s"($variable)-[:NEXT]->(next)"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          next <- (1 to 5).map(i => nodes((x.getId.toInt + i) % nodes.length))
        } yield Array(x, next),
      Seq("x", "next")
    )

  override def rhsOfApplyOperator(variable: String) =
    RHSOfApplyOneChildTD(
      _.expand(s"($variable)-[:NEXT]->(next)"),
      rowsComingIntoTheOperator =>
        for {
          Array(x, y) <- rowsComingIntoTheOperator
          next <- (1 to 5).map(i => nodes((y.getId.toInt + i) % nodes.length))
        } yield Array(x, y, next),
      Seq("x", "y", "next")
    )

  override def rhsOfCartesianOperator(variable: String) =
    RHSOfCartesianOneChildTD(
      _.expand(s"($variable)-[:NEXT]->(next)"),
      rowsComingIntoTheOperator =>
        for {
          Array(y) <- rowsComingIntoTheOperator
          next <- (1 to 5).map(i => nodes((y.getId.toInt + i) % nodes.length))
        } yield Array(y, next),
      Seq("y", "next")
    )
}

// EAGER AGGREGATION

class MorselAggregationTest extends AggregationTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselAggregationStressTest extends ParallelStressSuite /*with RHSOfApplyOneChildStressSuite with RHSOfCartesianOneChildStressSuite*/ with OnTopOfParallelInputStressTest {

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

class MorselFilterTest extends FilterTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselFilterStressTest extends ParallelStressSuite with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.filter(Seq(lessThan(varFor("prop"), literalInt(10)))),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator if x.getId < 10
        } yield Array(x),
      Seq("x")
    )
}

// PROJECTION
class MorselProjectionTest extends ProjectionTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselProjectionStressTest extends ParallelStressSuite with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.projection("prop * 2 AS j"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
        } yield Array(x, x.getId * 2),
      Seq("x", "j")
    )
}

// UNWIND
class MorselUnwindTest extends UnwindTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

class MorselUnwindStressTest extends ParallelStressSuite with OnTopOfParallelInputStressTest {

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
//class MorselArgumentTest extends ArgumentTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT)

//class MorselArgumentStressTest extends ParallelStressSuite with RHSOfApplyLeafStressSuite {
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

class MorselInputTest extends InputTestBase(ENTERPRISE_PARALLEL, MorselRuntime, SIZE_HINT) {

  test("should process input batches in parallel") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .input(variables = Seq("x"))
      .build()

    val input = inputSingleColumn(nBatches = SIZE_HINT, batchSize = 2, rowNumber => rowNumber)

    val result = executeUntil(logicalQuery, input, HasEvidenceOfParallelism)

    // then
    result should beColumns("x").withRows(input.flatten)
  }
}

// APPLY

class MorselApplyStressTest extends ParallelStressSuite {

  test("should support nested Apply") {
    // given
    init()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.apply()
      .|.|.nodeIndexOperator("c:Label(prop < ???)", paramExpr = Some(prop("b", "prop")), argumentIds = Set("a", "b"))
      .|.nodeIndexOperator("b:Label(prop < ???)", paramExpr = Some(prop("a", "prop")), argumentIds = Set("a"))
      .nodeIndexOperator("a:Label(prop <= 40)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- nodes if a.getId <= 40
      b <- nodes if b.getId < a.getId
      c <- nodes if c.getId < b.getId
    } yield Array(a, b, c)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }
}

// CARTESIAN PRODUCT

class MorselCartesianProductStressTest extends ParallelStressSuite {

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
