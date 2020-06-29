/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.Longs
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.Refs
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowR
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowRL
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.mockPipeFor
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.testableResult
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue

class ValueHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = SlotConfiguration.empty
    slotInfo.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo)


    val right = mock[Pipe]
    val pipe = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo,
      SlotMappings(Array((0,0)), Array((1,1)), Array.empty)
    )()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verifyNoInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = SlotConfiguration.empty
    slotInfo.newReference("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo, RowR(NO_VALUE))
    val right = mockPipeFor(slotInfo, RowR(intValue(42)))

    val pipe = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo,
      SlotMappings(Array((0,0)), Array((1,1)), Array.empty)
    )()

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

    val pipe = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(1), ReferenceFromSlot(1), left, right, slotInfoForJoin,
      SlotMappings(Array((0,0)), Array((0,0), (1,1), (1, 2)), Array.empty)
    )()

    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(
      List(Map("arg1" -> 42L, "arg2" -> intValue(666), "a" -> intValue(2), "b" -> intValue(2))))
  }

}
