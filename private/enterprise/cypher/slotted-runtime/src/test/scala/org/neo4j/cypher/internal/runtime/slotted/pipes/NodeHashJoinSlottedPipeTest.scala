/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.{RowL, mockPipeFor, testableResult}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.collection.immutable

class NodeHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should support simple hash join over nodes") {
    // given
    val node1 = 1
    val node2 = 2
    val node3 = 3
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty.newLong("b", nullable = false, CTNode)

    val left = mockPipeFor(slots, RowL(node1), RowL(node2))
    val right = mockPipeFor(slots, RowL(node2), RowL(node3))

    // when
    val result = NodeHashJoinSlottedPipe(Array(0), Array(0), left, right, slots, Array(), Array(), Array())().createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(Map("b" -> node2)))
  }

  test("should support joining on two different variables") {
    // given
    val queryState = QueryStateHelper.empty

    val leftSlots = SlotConfiguration.empty
    leftSlots.newLong("a", nullable = false, CTNode)
    leftSlots.newLong("b", nullable = false, CTNode)
    leftSlots.newLong("c", nullable = false, CTNode)
    val rightSlots = SlotConfiguration.empty
    rightSlots.newLong("a", nullable = false, CTNode)
    rightSlots.newLong("b", nullable = false, CTNode)
    rightSlots.newLong("d", nullable = false, CTNode)
    val hashSlots = SlotConfiguration.empty
    hashSlots.newLong("a", nullable = false, CTNode)
    hashSlots.newLong("b", nullable = false, CTNode)
    hashSlots.newLong("c", nullable = false, CTNode)
    hashSlots.newLong("d", nullable = false, CTNode)

    val left = mockPipeFor(leftSlots,
      RowL(node0, node1, node1),
      RowL(node0, node2, node2),
      RowL(node0, node2, node3),
      RowL(node1, node2, node4),
      RowL(node0, NULL, node5)
    )

    val right = mockPipeFor(rightSlots,
      RowL(node0, node1, node1),
      RowL(node0, node2, node2),
      RowL(node2, node2, node3),
      RowL(NULL, node2, node4)
    )

    // when
    val result = NodeHashJoinSlottedPipe(Array(0, 1), Array(0, 1), left, right, hashSlots, Array((2, 3)), Array(), Array())().
      createResults(queryState)

    // then
    testableResult(result, hashSlots).toSet should equal(Set(
      Map("a" -> node0, "b" -> node1, "c" -> node1, "d" -> node1),
      Map("a" -> node0, "b" -> node2, "c" -> node2, "d" -> node2),
      Map("a" -> node0, "b" -> node2, "c" -> node3, "d" -> node2)
    ))
  }

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
  private val node1 = 1
  private val node2 = 2
  private val node3 = 3
  private val node4 = 4
  private val node5 = 5
  private val NULL = -1

}
