/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.{Longs, Refs, RowR, RowRL, mockPipeFor, testableResult}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{NO_VALUE, intValue}

class ValueHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = SlotConfiguration.empty
    slotInfo.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo)


    val right = mock[Pipe]
    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo, Array.empty, Array.empty, Array.empty)()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verifyZeroInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = SlotConfiguration.empty
    slotInfo.newReference("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo, RowR(NO_VALUE))
    val right = mockPipeFor(slotInfo, RowR(intValue(42)))

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo, Array.empty, Array.empty, Array.empty)()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verify(right).createResults(any())
    verifyNoMoreInteractions(right)
  }

  test("should support hash join between two identifiers with shared arguments") {
    // given
    val queryState = QueryStateHelper.empty
    val slotInfoForInputs = SlotConfiguration.empty
      .newLong("arg1", nullable = false, CTNode)
      .newReference("arg2", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)

    val slotInfoForJoin = SlotConfiguration.empty
      .newLong("arg1", nullable = false, CTNode)
      .newReference("arg2", nullable = false, CTInteger)
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)

    val left = mockPipeFor(slotInfoForInputs,
      RowRL(Longs(42), Refs(intValue(666), intValue(1))),
      RowRL(Longs(42), Refs(intValue(666), intValue(2))),
      RowRL(Longs(42), Refs(intValue(666), NO_VALUE))
    )
    val right = mockPipeFor(slotInfoForInputs,
      RowRL(Longs(42), Refs(intValue(666), intValue(2))),
      RowRL(Longs(42), Refs(intValue(666), intValue(3))),
      RowRL(Longs(42), Refs(intValue(666), NO_VALUE))
    )

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(1), ReferenceFromSlot(1), left, right, slotInfoForJoin,
                                        Array((0,0)), Array((1,2)), Array.empty)()

    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(
      List(Map("arg1" -> 42L, "arg2" -> intValue(666), "a" -> intValue(2), "b" -> intValue(2))))
  }

}
