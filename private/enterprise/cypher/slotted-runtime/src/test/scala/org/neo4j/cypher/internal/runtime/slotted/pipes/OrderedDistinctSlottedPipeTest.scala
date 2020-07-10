/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection.DistinctSet

class OrderedDistinctSlottedPipeTest extends CypherFunSuite {
  test("exhaust should close seen set") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val slots = SlotConfiguration.empty.newLong("a", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a"->10)), slots)
    val pipe = OrderedDistinctSlottedPipe(input, slots, EmptyGroupingExpression, EmptyGroupingExpression)()
    // exhaust
    pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager)).toList
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size(1)
  }

  test("close should close seen set") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val slots = SlotConfiguration.empty.newLong("a", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a"->10)), slots)
    val pipe = OrderedDistinctSlottedPipe(input, slots, EmptyGroupingExpression, EmptyGroupingExpression)()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size(1)
  }
}
