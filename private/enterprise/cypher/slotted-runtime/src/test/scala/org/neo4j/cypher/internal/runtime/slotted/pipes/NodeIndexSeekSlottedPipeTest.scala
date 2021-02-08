/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UniqueIndexSeek
import org.neo4j.cypher.internal.runtime.slotted.SlottedCypherRowFactory
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.PropertyIndexQuery

class NodeIndexSeekSlottedPipeTest extends CypherFunSuite {
  test("exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(state.query.nodeIndexSeek(any[IndexReadSession], any[Boolean], any[IndexOrder], any[Seq[PropertyIndexQuery]])).thenAnswer((_: InvocationOnMock) => {
      //NOTE: this is what is done in TransactionBoundQueryContext
      resourceManager.trace(cursor)
      cursor
    })

    val pipe = NodeIndexSeekSlottedPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      IndexedSeq(SlottedIndexedProperty(0, None)),
      0,
      SingleQueryExpression(LiteralHelper.literal(42)),
      UniqueIndexSeek,
      IndexOrderNone,
      slots)()
    pipe.rowFactory = SlottedCypherRowFactory(slots, SlotConfiguration.Size.zero)
    // exhaust
    pipe.createResults(state).toList
    monitor.closedResources.collect { case `cursor` => cursor } should have size(1)
  }

  test("close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)

    val cursor = new StubNodeValueIndexCursor().withNode(0)
    when(state.query.nodeIndexSeek(any[IndexReadSession], any[Boolean], any[IndexOrder], any[Seq[PropertyIndexQuery]])).thenAnswer((_: InvocationOnMock) => {
      //NOTE: this is what is done in TransactionBoundQueryContext
      resourceManager.trace(cursor)
      cursor
    })
    val pipe = NodeIndexSeekSlottedPipe(
      "n",
      LabelToken("Awesome", LabelId(0)),
      IndexedSeq(SlottedIndexedProperty(0, None)),
      0,
      SingleQueryExpression(LiteralHelper.literal(42)),
      UniqueIndexSeek,
      IndexOrderNone,
      slots)()
    pipe.rowFactory = SlottedCypherRowFactory(slots, SlotConfiguration.Size.zero)
    val result = pipe.createResults(state)
    result.close()
    monitor.closedResources.collect { case `cursor` => cursor } should have size(1)
  }
}
