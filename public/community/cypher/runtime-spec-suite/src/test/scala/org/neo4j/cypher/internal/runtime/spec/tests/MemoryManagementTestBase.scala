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
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.exceptions.TransactionOutOfMemoryException
import org.neo4j.kernel.impl.util.ValueUtils

object MemoryManagementTestBase {
  // The configured max memory per transaction in Bytes
  val maxMemory = 4000L
}

trait InputStreams[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  /**
    * Infinite iterator.
    *
    * @param rowSize the size of a row in Bytes
    * @param data    an optionally empty array. If non-empty, it will be returned in every call to `next`. If non-empty, the iterator returns integer values.
    */
  protected def infiniteInput(rowSize: Long, data: Any*): InputDataStream = {
    iteratorInput(iterate(data.toArray, None, nodeInput = false, rowSize))
  }

  /**
    * Infinite iterator.
    *
    * @param rowSize the size of a row in Bytes
    * @param data    an optionally empty array. If non-empty, it will be returned in every call to `next`. If non-empty, the iterator returns node values.
    */
  protected def infiniteNodeInput(rowSize: Long, data: Any*): InputDataStream = {
    iteratorInput(iterate(data.toArray, None, nodeInput = true, rowSize))
  }

  /**
    * Finite iterator.
    *
    * @param limit the iterator will be exhausted after the given amount of rows
    * @param data  an optionally empty array. If non-empty, it will be returned in every call to `next`. If non-empty, the iterator returns integer values.
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
        val value = if (nodeInput) graphDb.createNode() else i
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
    GraphDatabaseSettings.transaction_max_memory -> 0L.asInstanceOf[java.lang.Long]), runtime) with InputStreams[CONTEXT] {
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
    GraphDatabaseSettings.transaction_max_memory -> MemoryManagementTestBase.maxMemory.asInstanceOf[java.lang.Long]), runtime) with InputStreams[CONTEXT] {

  test("should kill sort query before it runs out of memory") {
    // given
    val input = infiniteInput(estimateSize(E_INT))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct query before it runs out of memory") {
    // given
    val input = infiniteInput(estimateSize(E_INT_IN_DISTINCT))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should not kill count aggregation query") {
    // given
    val input = finiteInput(100000)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    consume(execute(logicalQuery, runtime, input))
  }

  test("should kill collect aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(estimateSize(E_INT))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - one large group") {
    // given
    val input = infiniteInput(estimateSize(E_INT), 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill grouping aggregation query before it runs out of memory - many groups") {
    // given
    val input = infiniteInput(estimateSize(E_INT))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill node hash join query before it runs out of memory") {
    // given
    val nodes = nodeGraph(1)
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE), nodes.head)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill multi-column node hash join query before it runs out of memory") {
    // given
    val (nodes, _) = circleGraph(1)
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE) * 2, nodes.head, nodes.head)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.allNodeScan("x")
      .input(nodes = Seq("x", "y"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}

/**
  * Tests for runtime with full language support
  */
trait FullSupportMemoryManagementTestBase [CONTEXT <: RuntimeContext] {
  self: MemoryManagementTestBase[CONTEXT] =>

  test("should kill eager query before it runs out of memory") {
    // given
    val input = infiniteInput(estimateSize(E_INT))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill stdDev aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(java.lang.Double.BYTES, 5) // StdDev stores primitive doubles

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("stdev(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileDisc aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(estimateSize(E_INT), 5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileDisc(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill percentileCont aggregation query before it runs out of memory") {
    // given
    val input = infiniteInput(estimateSize(E_INT), 5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("percentileCont(x, 0.1) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill distinct aggregation query before it runs out of memory") {
    // given
    val (nodes, _) = circleGraph(1) // Just for size estimation
    val input = infiniteNodeInput(estimateSize(E_NODE_VALUE))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(DISTINCT x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}
