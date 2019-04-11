/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, ZombieRuntime}
import org.neo4j.cypher.result.RuntimeResult

// INPUT
class ZombieInputTest extends ParallelInputTestBase(ZombieRuntime)

// ALL NODE SCAN
class ZombieAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieAllNodeScanStressTest extends AllNodeScanStressTestBase(ZombieRuntime)

// LABEL SCAN
class ZombieLabelScanTest extends LabelScanTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieLabelScanStressTest extends LabelScanStressTestBase(ZombieRuntime)

// INDEX SEEK
class ZombieNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class ZombieIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(ZombieRuntime)
class ZombieIndexSeekExactStressTest extends IndexSeekExactStressTest(ZombieRuntime)

// APPLY
class ZombieApplyStressTest extends ApplyStressTestBase(ZombieRuntime)

// EXPAND
class ZombieExpandAllTest extends ExpandAllTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieExpandStressTest extends ExpandStressTestBase(ZombieRuntime)

// PROJECTION
class ZombieProjectionTest extends ProjectionTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieProjectionStressTest extends ProjectionStressTestBase(ZombieRuntime)

// LIMIT
class ZombieLimitTest extends LimitTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)

// UNWIND
class ZombieUnwindTest extends UnwindTestBase(ENTERPRISE.PARALLEL, ZombieRuntime, SIZE_HINT)
class ZombieUnwindStressTest extends UnwindStressTestBase(ZombieRuntime)

class ZombieSingleThreadedTest extends ZombieTestSuite(ENTERPRISE.SINGLE_THREADED)
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
      .filter(Seq(
        greaterThanOrEqual(function("id", varFor("x")), literalInt(3))))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { n <- nodes if n.getId >= 3 } yield Array(n)
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should handle expand") {
    // given
    val (nodes, rels) = circleGraph(10000)

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
        result should beColumns("x").withRows(singleColumn(nodes))
      }
    }
  }
}
