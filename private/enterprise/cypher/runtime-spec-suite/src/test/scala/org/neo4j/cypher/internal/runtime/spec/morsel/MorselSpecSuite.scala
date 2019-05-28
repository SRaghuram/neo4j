/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending}
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.runtime.spec.{RowsMatcher, _}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, MorselRuntime}
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeReference, RelationshipReference}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

object MorselSpecSuite {
  val SIZE_HINT = 1000
}

// INPUT
class MorselInputTest extends ParallelInputTestBase(MorselRuntime)

// ALL NODE SCAN
class MorselAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselAllNodeScanStressTest extends AllNodeScanStressTestBase(MorselRuntime)

// LABEL SCAN
class MorselLabelScanTest extends LabelScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselLabelScanStressTest extends LabelScanStressTestBase(MorselRuntime)

// INDEX SEEK
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
class MorselNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class MorselIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(MorselRuntime)
class MorselIndexSeekExactStressTest extends IndexSeekExactStressTest(MorselRuntime)

// INDEX SCAN
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselIndexScanStressTest extends IndexScanStressTestBase(MorselRuntime)

// INDEX CONTAINS SCAN
class MorselNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselIndexContainsScanStressTest extends IndexContainsScanStressTestBase(MorselRuntime)

// ARGUMENT
class MorselArgumentTest extends ArgumentTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselArgumentStressTest extends ArgumentStressTestBase(MorselRuntime)

// APPLY
class MorselApplyStressTest extends ApplyStressTestBase(MorselRuntime)

// EXPAND
class MorselExpandAllTest extends ExpandAllTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselExpandStressTest extends ExpandStressTestBase(MorselRuntime)

// PROJECTION
class MorselProjectionTest extends ProjectionTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselProjectionStressTest extends ProjectionStressTestBase(MorselRuntime)

// FILTER
class MorselFilterTest extends FilterTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselFilterNoFusingTest extends FilterTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselFilterStressTest extends FilterStressTestBase(MorselRuntime)

// LIMIT
class MorselLimitTest extends LimitTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselLimitNoFusingTest extends LimitTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)

// UNWIND
class MorselUnwindTest extends UnwindTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselUnwindStressTest extends UnwindStressTestBase(MorselRuntime)

// SORT
class MorselSortTest extends SortTestBase(ENTERPRISE.PARALLEL, MorselRuntime, 1000)

// AGGREGATION
class MorselSingleThreadedAggregationTest extends AggregationTestBase(ENTERPRISE.SINGLE_THREADED, MorselRuntime, SIZE_HINT)
// TODO Uncomment after parallel keeps input order
//class MorselParallelAggregationTest extends AggregationTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselAggregationStressTest extends AggregationStressTestBase(MorselRuntime)

// NODE HASH JOIN
class MorselNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.PARALLEL, MorselRuntime, 1000)

// REACTIVE
class MorselReactiveSingleThreadedTest extends ReactiveResultTestBase(ENTERPRISE.SINGLE_THREADED, MorselRuntime)
class MorselReactiveSingleThreadedNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.SINGLE_THREADED_NO_FUSING, MorselRuntime)
class MorselReactiveParallelTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL, MorselRuntime)
class MorselReactiveParallelNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime)
class MorselReactiveSingleThreadedStressTest extends ReactiveResultStressTestBase(ENTERPRISE.SINGLE_THREADED, MorselRuntime, SIZE_HINT)
class MorselReactiveSingleThreadedNoFusingStressTest extends ReactiveResultStressTestBase(ENTERPRISE.SINGLE_THREADED_NO_FUSING, MorselRuntime, SIZE_HINT)
class MorselReactiveParallelStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.PARALLEL, MorselRuntime,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size
class MorselReactiveParallelNoFusingStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size

// GENERAL
class MorselSingleThreadedTest extends MorselTestSuite(ENTERPRISE.SINGLE_THREADED)
class MorselSingleThreadedNoFusingTest extends MorselTestSuite(ENTERPRISE.SINGLE_THREADED_NO_FUSING)
class MorselParallelTest extends MorselTestSuite(ENTERPRISE.PARALLEL)
class MorselParallelNoFusingTest extends MorselTestSuite(ENTERPRISE.PARALLEL_NO_FUSING)
class MorselSchedulerTracerTest extends SchedulerTracerTestBase(MorselRuntime)

// WORKLOAD
class MorselParallelWorkloadTest extends WorkloadTestBase(ENTERPRISE.PARALLEL, MorselRuntime, SIZE_HINT)
class MorselParallelNoFusingWorkloadTest extends WorkloadTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MorselRuntime, SIZE_HINT)

abstract class MorselTestSuite(edition: Edition[EnterpriseRuntimeContext]) extends RuntimeTestSuite(edition, MorselRuntime) {

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

  test("should complete query with error and close cursors") {
    nodePropertyGraph(SIZE_HINT, {
      case i => Map("prop" -> (i - (SIZE_HINT / 2)))
    })

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filter("100/n.prop = 1") // will explode!
      .allNodeScan("n")
      .build()

    // when
    import scala.concurrent.ExecutionContext.global
    val futureResult = Future(consume(execute(logicalQuery, runtime)))(global)

    // then
    intercept[org.neo4j.cypher.internal.v4_0.util.ArithmeticException] {
      Await.result(futureResult, 30.seconds)
    }
  }

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
