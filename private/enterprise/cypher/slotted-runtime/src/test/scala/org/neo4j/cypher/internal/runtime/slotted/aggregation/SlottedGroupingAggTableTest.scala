/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CountStar
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes.FakeSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlotExpression
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedGroupingExpression1
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap

class SlottedGroupingAggTableTest extends CypherFunSuite {

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val slots = SlotConfiguration.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("c", nullable = false, CTInteger)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val table = new SlottedGroupingAggTable(
      slots,
      SlottedGroupingExpression1(SlotExpression(slots("a"), ReferenceFromSlot(0))),
      Map(slots("c").offset -> CountStar()),
      state,
      Id(0))
    table.clear()

    val input = FakeSlottedPipe(Seq(Map("a"->1), Map("a"->1), Map("a"->2), Map("a"->2)), slots).createResults(state)
    table.processRow(input.next())
    table.processRow(input.next())
    table.processRow(input.next())
    table.processRow(input.next())

    // when
    val iter = table.result()
    iter.close()

    // then
    monitor.closedResources.collect { case t: HeapTrackingOrderedAppendMap[_, _] => t } should have size 1
  }
}
