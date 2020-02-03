/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.verifyZeroInteractions
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowL
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.mockPipeFor
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable

class NodeHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty
    slots.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slots)


    val right = mock[Pipe]

    // when
    val result = NodeHashJoinSlottedPipe(Array(0, 1), Array(0, 1), left, right, slots, Array(), Array(), Array())().
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
    val result = NodeHashJoinSlottedPipe(Array(0, 1), Array(0, 1), left, right, slots, Array(), Array(), Array())().
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
    val a_b: immutable.Seq[RowL] = (0 to size) map { i =>
      RowL(i.toLong, i.toLong)
    }
    val b_c: immutable.Seq[RowL] = (size+1 to size*2) map { i =>
      RowL(i.toLong, i.toLong)
    }

    val lhs = SlotConfiguration.empty
    lhs.newLong("a", nullable = false, CTNode)
    lhs.newLong("b", nullable = false, CTNode)

    val rhs = SlotConfiguration.empty
    rhs.newLong("b", nullable = false, CTNode)
    rhs.newLong("c", nullable = false, CTNode)

    val output = SlotConfiguration.empty
    output.newLong("a", nullable = false, CTNode)
    output.newLong("b", nullable = false, CTNode)
    output.newLong("c", nullable = false, CTNode)

    val lhsPipe = mockPipeFor(lhs, a_b:_*)
    val rhsPipe = mockPipeFor(lhs, b_c:_*)

    // when
    val result = NodeHashJoinSlottedPipe(
      lhsOffsets = Array(0, 1),
      rhsOffsets = Array(0, 1),
      left = lhsPipe,
      right = rhsPipe,
      slots = output,
      longsToCopy = Array((1, 2)),
      refsToCopy = Array(),
      cachedPropertiesToCopy = Array())().
      createResults(QueryStateHelper.empty)

    // If we got here it means we did not throw a stack overflow exception. ooo-eeh!
    result should be(empty)
  }

  private val node0 = 0
  private val NULL = -1

}
