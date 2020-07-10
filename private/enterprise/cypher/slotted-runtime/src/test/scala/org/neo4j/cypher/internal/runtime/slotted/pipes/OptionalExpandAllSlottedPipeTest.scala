/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes
import org.mockito.Mockito
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerTypes
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain

class OptionalExpandAllSlottedPipeTest extends CypherFunSuite {
  test("exhaust should close buffer") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = mock[NodeCursor]
    val relCursor = new StubRelationshipCursor(new TestRelationshipChain(0))
    Mockito.when(nodeCursor.next()).thenReturn(true, false)
    Mockito.when(state.query.traversalCursor()).thenReturn(relCursor)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("r", nullable = true, CTRelationship)
      .newLong("b", nullable = true, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a"->10)), slots)
    val pipe = OptionalExpandAllSlottedPipe(input, slots("a"), 1, 2, SemanticDirection.OUTGOING, new EagerTypes(Array(0)), slots, None)()
    // exhaust
    pipe.createResults(state).toList
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case r@`relCursor` => r } should have size(1)
  }

  test("close should close buffer") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = new StubNodeCursor().withNode(0)
    val relCursor = new StubRelationshipCursor(new TestRelationshipChain(0).outgoing(0, 1, 0))
    Mockito.when(state.query.traversalCursor()).thenReturn(relCursor)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("r", nullable = true, CTRelationship)
      .newLong("b", nullable = true, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a"->10)), slots)
    val pipe = OptionalExpandAllSlottedPipe(input, slots("a"), 1, 2, SemanticDirection.OUTGOING, new EagerTypes(Array(0)), slots, None)()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize to get cursor registered
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case r@`relCursor` => r } should have size(1)
  }
}
