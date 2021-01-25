/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerTypes
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.virtual.RelationshipValue

class VarLengthExpandSlottedPipeTest extends CypherFunSuite {
  private trait WasClosed {
    def wasClosed: Boolean
  }

  private def relationshipIterator: ClosingLongIterator with RelationshipIterator with WasClosed =
    new ClosingLongIterator with RelationshipIterator with WasClosed {
    private val inner = Iterator(1L, 2L, 3L)
    private var _wasClosed = false

    override def close(): Unit = _wasClosed = true

    override def wasClosed: Boolean = _wasClosed

    override protected[this] def innerHasNext: Boolean = inner.hasNext

    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long,
                                                           visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
      visitor.visit(relationshipId, -1, -1, -1)
      true
    }

    override def next(): Long = inner.next()
  }

  private def relationshipValue(id: Long): RelationshipValue = {
    val r = mock[RelationshipValue]
    Mockito.when(r.id).thenReturn(id)
    r
  }

  test("exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = mock[NodeCursor]
    Mockito.when(nodeCursor.next()).thenReturn(true, false)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)
    val rels = relationshipIterator
    Mockito.when(state.query.getRelationshipsForIdsPrimitive(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(rels)

    Mockito.when(state.query.relationshipById(any[Long], any[Long], any[Long], any[Int])).thenAnswer(
      (invocation: InvocationOnMock) => relationshipValue(invocation.getArgument[Long](0)))

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newReference("r", nullable = false, CTList(CTRelationship))
      .newLong("b", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a"->10)), slots)
    val pipe = VarLengthExpandSlottedPipe(input,
      slots("a"),
      slots("r").offset,
      slots("b"),
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      1,
      None,
      shouldExpandAll = true,
      slots,
      VariablePredicates.NO_PREDICATE_OFFSET,
      VariablePredicates.NO_PREDICATE_OFFSET,
      True(),
      True(),
      SlotConfiguration.Size(0, 0))()
    // exhaust
    pipe.createResults(state).toList
    input.wasClosed shouldBe true
    rels.wasClosed shouldBe true
  }

  test("close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeCursor = mock[NodeCursor]
    Mockito.when(nodeCursor.next()).thenReturn(true, false)
    Mockito.when(state.query.nodeCursor()).thenReturn(nodeCursor)
    val rels = relationshipIterator
    Mockito.when(state.query.getRelationshipsForIdsPrimitive(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(rels)

    Mockito.when(state.query.relationshipById(any[Long], any[Long], any[Long], any[Int])).thenAnswer(
      (invocation: InvocationOnMock) => relationshipValue(invocation.getArgument[Long](0)))

    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newReference("r", nullable = false, CTList(CTRelationship))
      .newLong("b", nullable = false, CTNode)

    val input = FakeSlottedPipe(Seq(Map("a"->10)), slots)
    val pipe = VarLengthExpandSlottedPipe(input,
      slots("a"),
      slots("r").offset,
      slots("b"),
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      new EagerTypes(Array(0)),
      1,
      None,
      shouldExpandAll = true,
      slots,
      VariablePredicates.NO_PREDICATE_OFFSET,
      VariablePredicates.NO_PREDICATE_OFFSET,
      True(),
      True(),
      SlotConfiguration.Size(0, 0))()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize to get cursor registered
    result.close()
    input.wasClosed shouldBe true
    rels.wasClosed shouldBe true
  }
}
