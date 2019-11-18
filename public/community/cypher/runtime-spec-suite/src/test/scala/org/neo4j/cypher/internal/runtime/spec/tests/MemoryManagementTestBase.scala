/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, LogicalQuery, RuntimeContext}
import org.neo4j.exceptions.TransactionOutOfMemoryException
import org.neo4j.kernel.impl.util.ValueUtils

object MemoryManagementTestBase {
  // The configured max memory per transaction in Bytes
  val maxMemory = 4000L
}

trait InputStreams[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  /**
   * Finite iterator.
   *
   * @param limit the iterator will be exhausted after the given amount of rows
   * @param rowSize the size of a row in Bytes
   * @param data    an optionally empty array. If non-empty, it will be returned in every call to `next`. If empty, the iterator returns integer values.
   */
  protected def finiteInput(limit: Int, rowSize: Long, data: Any*): InputDataStream = {
    iteratorInput(iterate(data.toArray, Some(limit), nodeInput = false, rowSize))
  }

  /**
    * Infinite iterator.
    *
    * @param rowSize the size of a row in Bytes
    * @param data    an optionally empty array. If non-empty, it will be returned in every call to `next`. If empty, the iterator returns integer values.
    */
  protected def infiniteInput(rowSize: Long, data: Any*): InputDataStream = {
    iteratorInput(iterate(data.toArray, None, nodeInput = false, rowSize))
  }

  /**
    * Infinite iterator.
    *
    * @param rowSize the size of a row in Bytes
    * @param data    an optionally empty array. If non-empty, it will be returned in every call to `next`. If empty, the iterator returns node values.
    */
  protected def infiniteNodeInput(rowSize: Long, data: Any*): InputDataStream = {
    iteratorInput(iterate(data.toArray, None, nodeInput = true, rowSize))
  }

  /**
    * Finite iterator.
    *
    * @param limit the iterator will be exhausted after the given amount of rows
    * @param data  an optionally empty array. If non-empty, it will be returned in every call to `next`. If empty, the iterator returns integer values.
    */
  protected def finiteInput(limit: Int, data: Any*): InputDataStream = {
    iteratorInput(iterate(data.toArray, Some(limit), nodeInput = false, -1))
  }

  /**
    * Determine after how many rows to kill the query and fail the test, given the size of a row in the operator under test.
    * @param rowSize the size of a row in Bytes.
    */
  protected def killAfterNRows(rowSize: Long): Long = {
    MemoryManagementTestBase.maxMemory / rowSize + 10 // An extra of 10 rows to account for mis-estimation and batching
  }

  sealed trait ValueToEstimate
  // a single int column
  case object E_INT extends ValueToEstimate
  // a single int column used in DISTINCT
  case object E_INT_IN_DISTINCT extends ValueToEstimate
  // a single node column, which can be stored in a long-slot in slotted
  case object E_NODE_PRIMITIVE extends ValueToEstimate
  // a single node column, which cannot be stored in a long-slot in slotted
  case object E_NODE_VALUE extends ValueToEstimate

  /**
    * Estimate the size of an object after converting it into a Neo4j value.
    */
  protected def estimateSize(data: ValueToEstimate): Long = {
    data match {
      case E_INT => ValueUtils.of(0).estimatedHeapUsage()
      case E_INT_IN_DISTINCT => ValueUtils.of(java.util.Arrays.asList(0)).estimatedHeapUsage() // We wrap the columns in a list
      case E_NODE_PRIMITIVE => 64  // Size of a NodeValue
      case E_NODE_VALUE => 64  // Size of a NodeValue
    }
  }

  /**
    * Create an iterator.
    *
    * @param data      an optionally empty array. If non-empty, it will be returned in every call to `next`
    * @param limit     if defined, the iterator will be exhausted after the given amount of rows
    * @param nodeInput if true, and data is empty, the iterator returns node values.
    *                  If false, and data is empty, the iterator returns integer values.
    * @param rowSize   the size of a row in the operator under test. This value determines when to fail the test if the query is not killed soon enough.
    */
  protected def iterate(data: Array[Any],
                        limit: Option[Int],
                        nodeInput: Boolean,
                        rowSize: Long): Iterator[Array[Any]] = new Iterator[Array[Any]] {
    private val killThreshold = killAfterNRows(rowSize)
    private var i = 0L
    override def hasNext: Boolean = limit.fold(true)(i < _)

    override def next(): Array[Any] = {
      i += 1
      if (limit.isEmpty && i > killThreshold) {
        fail("The query was not killed even though it consumed too much memory.")
      }
      if (data.isEmpty) {
        // Make sure that if you ever call this in parallel, you cannot just create nodes here and need to redesign the test.
        val value = if (nodeInput) runtimeTestSupport.tx.createNode() else i
        Array(value)
      } else {
        data
      }
    }
  }
}

abstract class MemoryManagementDisabledTestBase[CONTEXT <: RuntimeContext](
                                                                            edition: Edition[CONTEXT],
                                                                            runtime: CypherRuntime[CONTEXT]
                                                                          )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.track_query_allocation -> java.lang.Boolean.TRUE,
    GraphDatabaseSettings.query_max_memory -> Long.box(0L)), runtime) with InputStreams[CONTEXT] {
  test("should not kill memory eating query") {
    // given
    val input = finiteInput(10000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }
}

abstract class MemoryManagementTestBase[CONTEXT <: RuntimeContext](
                                                                    edition: Edition[CONTEXT],
                                                                    runtime: CypherRuntime[CONTEXT]
                                                                  )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.track_query_allocation -> java.lang.Boolean.TRUE,
    GraphDatabaseSettings.query_max_memory -> Long.box(MemoryManagementTestBase.maxMemory)), runtime) with InputStreams[CONTEXT] {

  test("should kill sort query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT_IN_DISTINCT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill count aggregation query") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = finiteInput(100000)

    // then
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill collect aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - one large group") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT, Some(0))
    val input = infiniteInput(expectedRowSize, 0)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - many groups") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill node hash join query before it runs out of memory") {
    // given
    val nodes = nodeGraph(1)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = infiniteInput(expectedRowSize, nodes.head)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill hash join query with large RHS") {
    // given
    val nodes = nodeGraph(100000)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_NODE_PRIMITIVE, Some(nodes.head))
    val input = finiteInput(1, expectedRowSize, nodes.head)

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill multi-column node hash join query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.allNodeScan("x")
      .input(nodes = Seq("x", "y"))
      .build()

    // when
    val (nodes, _) = circleGraph(1)
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE) * 2, nodes.head, nodes.head)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill double collect aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c1", "c2")
      .aggregation(Seq.empty, Seq("collect(x) AS c1", "collect(x) AS c2"))
      .input(variables = Seq("x"))
      .build()

    val estimatedRowSize = estimateRowSize(logicalQuery)

    // when
    val input = infiniteInput(estimatedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct + node hash join query before it runs out of memory") {
    // given
    val nodes = nodeGraph(1)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .distinct("x AS x")
      .input(nodes = Seq("x"))
      .build()

    val estimatedRowSize = estimateRowSize(logicalQuery, Some(nodes.head))

    // when
    val input = infiniteNodeInput(estimatedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill top query with low limit") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Ascending("x")), 10)
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = finiteInput(1000000, expectedRowSize)

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should not kill cartesian product query") {
    // given
    val nodes = nodeGraph(1)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .cartesianProduct()
      .|.allNodeScan("a")
      .input(variables = Seq("x"))
      .build()

    // when
    // Not running assertTotalAllocatedMemory since interpreted and slotted do not eagerize at all
    val expectedRowSize = estimateSize(E_INT) + estimateSize(E_NODE_PRIMITIVE)
    val input = finiteInput(100000, expectedRowSize)

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should not kill cartesian product query with large RHS") {
    // given
    nodeGraph(100000)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .cartesianProduct()
      .|.allNodeScan("a")
      .input(variables = Seq("x"))
      .build()

    // when
    // Not running assertTotalAllocatedMemory since interpreted and slotted do not eagerize at all
    val expectedRowSize = estimateSize(E_INT) + estimateSize(E_NODE_PRIMITIVE)
    val input = finiteInput(1, expectedRowSize)

    // then no exception
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill top n query before it runs out of memory, where n < Int.MaxValue") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Ascending("x")), Int.MaxValue - 1)
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill top n query before it runs out of memory, where n > Int.MaxValue") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Ascending("x")), Int.MaxValue + 1L)
      .input(variables = Seq("x"))
      .build()

    // when
    val expectedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = infiniteInput(expectedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  protected def assertTotalAllocatedMemory(logicalQuery: LogicalQuery, valueToEstimate: ValueToEstimate, sampleValue: Option[Any] = None): Long = {
    val expectedRowSize = estimateSize(valueToEstimate)
    val estimatedRowSize = estimateRowSize(logicalQuery, sampleValue)
    estimatedRowSize should be >= expectedRowSize
    estimatedRowSize should be < expectedRowSize * 10 // in pipelined we have lot's of overhead for some operators in corner cases
    expectedRowSize
  }

  protected def estimateRowSize(logicalQuery: LogicalQuery, sampleValue: Option[Any] = None): Long = {
    val nRows = 8
    val result = execute(logicalQuery, runtime, inputColumns(1, nRows, i => sampleValue.getOrElse(i)))
    consume(result)
    result.runtimeResult.totalAllocatedMemory().get() / nRows
  }
}

/**
  * Tests for runtime with full language support
  */
trait FullSupportMemoryManagementTestBase [CONTEXT <: RuntimeContext] {
  self: MemoryManagementTestBase[CONTEXT] =>

  test("should kill eager query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .input(variables = Seq("x"))
      .build()

    // when
    val estimatedRowSize = assertTotalAllocatedMemory(logicalQuery, E_INT)
    val input = infiniteInput(estimatedRowSize)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill stdDev aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("stdev(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = infiniteInput(java.lang.Double.BYTES, 5) // StdDev stores primitive doubles

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileDisc aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileDisc(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT), 5)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileCont aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileCont(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val input = infiniteInput(estimateSize(E_INT), 5)

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct aggregation query before it runs out of memory") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    // when
    val (nodes, _) = circleGraph(1) // Just for size estimation
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE))

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}
