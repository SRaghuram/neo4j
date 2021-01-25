/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeRightOuterHashJoinPipe
import org.neo4j.cypher.internal.runtime.slotted.SlottedCypherRowFactory
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowL
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.mockPipeFor
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.testableResult
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeRightOuterHashJoinFallbackPipeTest extends CypherFunSuite {

  test("should support joining on two different variables") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val leftSlots = SlotConfiguration.empty
    leftSlots.newLong("a", nullable = false, CTNode)
    leftSlots.newLong("b", nullable = false, CTNode)
    leftSlots.newLong("c", nullable = true, CTNode)
    SlotConfigurationUtils.generateSlotAccessorFunctions(leftSlots)

    val rightSlots = SlotConfiguration.empty
    rightSlots.newLong("a", nullable = true, CTNode)
    rightSlots.newLong("b", nullable = false, CTNode)
    rightSlots.newLong("d", nullable = false, CTNode)
    SlotConfigurationUtils.generateSlotAccessorFunctions(rightSlots)

    val hashSlots = SlotConfiguration.empty
    hashSlots.newLong("a", nullable = true, CTNode)
    hashSlots.newLong("b", nullable = false, CTNode)
    hashSlots.newLong("d", nullable = false, CTNode)
    hashSlots.newLong("c", nullable = true, CTNode)
    SlotConfigurationUtils.generateSlotAccessorFunctions(hashSlots)

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

    val pipe = NodeRightOuterHashJoinPipe(Set("a", "b"), left, right, Set("c"))()
    pipe.rowFactory = SlottedCypherRowFactory(hashSlots, SlotConfiguration.Size.zero)

    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, hashSlots).toSet should equal(Set(
      Map("a" -> node0, "b" -> node1, "c" -> node1, "d" -> node1),
      Map("a" -> node0, "b" -> node2, "c" -> node2, "d" -> node2),
      Map("a" -> node0, "b" -> node2, "c" -> node3, "d" -> node2),
      Map("a" -> node2, "b" -> node2, "c" -> NULL, "d" -> node3),
      Map("a" -> NULL, "b" -> node2, "c" -> NULL, "d" -> node4)
    ))
  }

  private val node0 = 0
  private val node1 = 1
  private val node2 = 2
  private val node3 = 3
  private val node4 = 4
  private val node5 = 5
  private val NULL = -1

}
