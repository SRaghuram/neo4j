/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, verifyZeroInteractions}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.{RowL, mockPipeFor}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.collection.immutable

class NodeHashJoinSlottedSingleNodePipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty
    slots.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slots)


    val right = mock[Pipe]

    // when
    val result = NodeHashJoinSlottedSingleNodePipe(0, 0, left, right, slots, Array(), Array(), Array())().
      createResults(queryState)

    // then
    result should be(empty)
    verifyZeroInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty
    slots.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slots, RowL(NULL))
    val right = mockPipeFor(slots, RowL(node0))

    // when
    val result = NodeHashJoinSlottedSingleNodePipe(0, 0, left, right, slots, Array(), Array(), Array())().
      createResults(queryState)

    // then
    result should be(empty)
    verify(right).createResults(any())
    verifyNoMoreInteractions(right)
  }

  test("worst case scenario should not lead to stackoverflow errors") {
    // This test case lead to stack overflow errors.
    // It's the worst case - large inputs on both sides that have no overlap on the join column
    val size = 10000
    val n1_to_1000: immutable.Seq[RowL] = (0 to size) map { i =>
      RowL(i.toLong)
    }
    val n1001_to_2000: immutable.Seq[RowL] = (size+1 to size*2) map { i =>
      RowL(i.toLong)
    }

    val slotConfig = SlotConfiguration.empty
    slotConfig.newLong("a", nullable = false, CTNode)

    val lhsPipe = mockPipeFor(slotConfig, n1_to_1000:_*)
    val rhsPipe = mockPipeFor(slotConfig, n1001_to_2000:_*)

    // when
    val result = NodeHashJoinSlottedSingleNodePipe(
      lhsOffset = 0,
      rhsOffset = 0,
      left = lhsPipe,
      right = rhsPipe,
      slotConfig,
      longsToCopy = Array(),
      refsToCopy = Array(),
      cachedPropertiesToCopy = Array())().
      createResults(QueryStateHelper.empty)

    // If we got here it means we did not throw a stack overflow exception. ooo-eeh!
    result should be(empty)
  }

  private val node0 = 0
  private val NULL = -1

}

