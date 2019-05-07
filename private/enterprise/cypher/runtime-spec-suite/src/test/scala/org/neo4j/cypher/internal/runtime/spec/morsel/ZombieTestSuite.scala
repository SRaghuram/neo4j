/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending}
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.runtime.spec.{RowsMatcher, _}
import org.neo4j.cypher.internal.runtime.zombie.ZombieRuntime
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeReference, RelationshipReference}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

// INPUT
class ZombieInputTest extends ParallelInputTestBase(ZombieRuntime)

// ALL NODE SCAN
class ZombieAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieAllNodeScanStressTest extends AllNodeScanStressTestBase(ZombieRuntime)

// LABEL SCAN
class ZombieLabelScanTest extends LabelScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieLabelScanStressTest extends LabelScanStressTestBase(ZombieRuntime)

// INDEX SEEK
class ZombieNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
class ZombieNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class ZombieIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(ZombieRuntime)
class ZombieIndexSeekExactStressTest extends IndexSeekExactStressTest(ZombieRuntime)

// INDEX SCAN
class ZombieNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieIndexScanStressTest extends IndexScanStressTestBase(ZombieRuntime)

// INDEX CONTAINS SCAN
class ZombieNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieIndexContainsScanStressTest extends IndexContainsScanStressTestBase(ZombieRuntime)

// ARGUMENT
class ZombieArgumentTest extends ArgumentTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieArgumentStressTest extends ArgumentStressTestBase(ZombieRuntime)

// APPLY
class ZombieApplyStressTest extends ApplyStressTestBase(ZombieRuntime)

// EXPAND
class ZombieExpandAllTest extends ExpandAllTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieExpandStressTest extends ExpandStressTestBase(ZombieRuntime)

// PROJECTION
class ZombieProjectionTest extends ProjectionTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieProjectionStressTest extends ProjectionStressTestBase(ZombieRuntime)

// FILTER
class ZombieFilterTest extends FilterTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieFilterNoFusingTest extends FilterTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieFilterStressTest extends FilterStressTestBase(ZombieRuntime)

// LIMIT
class ZombieLimitTest extends LimitTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieLimitNoFusingTest extends LimitTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)

// UNWIND
class ZombieUnwindTest extends UnwindTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime, SIZE_HINT)
class ZombieUnwindStressTest extends UnwindStressTestBase(ZombieRuntime)

// SORT
class ZombieSortTest extends SortTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, 1000)

// AGGREGATION
class ZombieSingleThreadedAggregationTest extends AggregationTestBase(ENTERPRISE.SINGLE_THREADED, ZombieRuntime, SIZE_HINT)
class ZombieParallelAggregationTest extends AggregationTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)

// NODE HASH JOIN
class ZombieNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, 1000)

// REACTIVE
class ZombieReactiveSingleThreadedTest extends ReactiveResultTestBase(ENTERPRISE.SINGLE_THREADED, ZombieRuntime)
class ZombieReactiveSingleThreadedNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.SINGLE_THREADED_NO_FUSING, ZombieRuntime)
class ZombieReactiveParallelTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL, ZombieRuntime)
class ZombieReactiveParallelNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL_NO_FUSING, ZombieRuntime)

class ZombieSingleThreadedTest extends ZombieTestSuite(ENTERPRISE.SINGLE_THREADED)
class ZombieSingleThreadedNoFusingTest extends ZombieTestSuite(ENTERPRISE.SINGLE_THREADED_NO_FUSING)
class ZombieParallelTest extends ZombieTestSuite(ENTERPRISE.PARALLEL)
class ZombieSchedulerTracerTest extends SchedulerTracerTestBase(ZombieRuntime)

abstract class ZombieTestSuite(edition: Edition[EnterpriseRuntimeContext]) extends RuntimeTestSuite(edition, ZombieRuntime) {

  test("should handle allNodeScan") {
    // given
    val nodes = nodeGraph(11)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { n <- nodes } yield Array(n)
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should handle allNodeScan and filter") {
    // given
    val nodes = nodeGraph(11)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("id(x) >= 3")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { n <- nodes if n.getId >= 3 } yield Array(n)
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should handle expand + filter") {
    // given
    val size = 1000
    val (_, rels) = circleGraph(size)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter(s"id(y) >= ${size / 2}")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        if r.getEndNode.getId >= size /2
        row <- List(Array(r.getStartNode, r.getEndNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand") {
    // given
    val (_, rels) = circleGraph(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getStartNode, r.getEndNode),
                    Array(r.getEndNode, r.getStartNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should sort") {
    // given
    circleGraph(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort(Seq(Descending("y")))
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(sortedDesc("y"))
  }

  test("should reduce twice in a row") {
    // given
    val nodes = nodeGraph(1000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Ascending("x")))
      .sort(sortItems = Seq(Descending("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes))
  }

  test("should all node scan and sort on rhs of apply") {
    // given
    val nodes = nodeGraph(10)
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.sort(sortItems = Seq(Descending("x")))
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  // TODO  Sort-Apply-Sort-Bug: re-enable
  ignore("should sort on top of apply with all node scan and sort on rhs of apply") {
    // given
    val nodes = nodeGraph(10)
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Descending("x")))
      .apply()
      .|.sort(sortItems = Seq(Descending("x")))
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  test("should apply-sort") {
    // given
    circleGraph(1000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort(Seq(Descending("y")))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(groupedBy("x").desc("y"))
  }

  test("should apply-apply-sort") {
    // given
    circleGraph(1000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.apply()
      .|.|.sort(Seq(Ascending("z")))
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.sort(Seq(Descending("y")))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(groupedBy("x", "y").asc("z"))
  }

  test("should deal with concurrent queries") {
    // given
    val nodes = nodeGraph(10)
    val executor = Executors.newFixedThreadPool(8)
    val QUERIES_PER_THREAD = 50

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val futureResultSets = (0 until 8).map(_ =>
      executor.submit(new Callable[Seq[RuntimeResult]] {
        override def call(): Seq[RuntimeResult] = {
          for (_ <- 0 until QUERIES_PER_THREAD) yield execute(logicalQuery, runtime)
        }
      })
    )

    // then
    for (futureResultSet <- futureResultSets) {

      val resultSet = futureResultSet.get(1, TimeUnit.MINUTES)
      resultSet.size should be(QUERIES_PER_THREAD)
      for (result <- resultSet) {
        inTx(
          result should beColumns("x").withRows(singleColumn(nodes))
        )
      }
    }
  }

  test("should complete query with error") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x = 0/0") // will explode!
      .input(variables = Seq("x"))
      .build()

    // when
    import scala.concurrent.ExecutionContext.global
    val futureResult = Future(consume(execute(logicalQuery, runtime, inputValues(Array(1)))))(global)

    // then
    intercept[org.neo4j.cypher.internal.v4_0.util.ArithmeticException] {
      Await.result(futureResult, 10.seconds)
    }
  }

  //NOTE: this could maybe be removed once morsel and zombie are merged since then
  //      we get test coverage also from PrePopulateAcceptanceTests. However this might
  //      be nice to have since it will test both with fusing enabled and disabled.
  test("should prepopulate results") {
    // given
    circleGraph(11)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandAll("(x)-[r]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(populated)
  }

  case object populated extends RowsMatcher {
    override def toString: String = "All entities should have been populated"
    override def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = {
      rows.forall(row => row.forall {
        case _: NodeReference => false
        case n: NodeProxyWrappingNodeValue => n.isPopulated
        case _ : RelationshipReference => false
        case r: RelationshipProxyWrappingValue => r.isPopulated
        case _ => true
      })
    }
  }
}
