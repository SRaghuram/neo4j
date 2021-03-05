/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexIteratorBase
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.storable.TextValue

abstract class AbstractRelationshipIndexStringScanSlottedPipe(ident: String,
                                                              startNode: String,
                                                              endNode: String,
                                                              property: SlottedIndexedProperty,
                                                              queryIndexId: Int,
                                                              valueExpr: Expression,
                                                              slots: SlotConfiguration) extends Pipe with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)
  override val indexPropertySlotOffsets: Array[Int] = property.maybeCachedNodePropertySlot.toArray
  override val indexPropertyIndices: Array[Int] = if (property.maybeCachedNodePropertySlot.isDefined) Array(0) else Array.empty
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val baseContext = state.newRowWithArgument(rowFactory)
    val value = valueExpr(baseContext, state)

    val resultNodes = value match {
      case value: TextValue =>
       iterator(state, slots.getLongOffsetFor(startNode), slots.getLongOffsetFor(endNode), baseContext, queryContextCall(state, state.queryIndexes(queryIndexId), value))
      case IsNoValue() =>
        ClosingIterator.empty
      case x => throw new CypherTypeException(s"Expected a string value, but got $x")
    }

    resultNodes
  }

  protected def queryContextCall(state: QueryState,
                                 index: IndexReadSession,
                                 value: TextValue): RelationshipValueIndexCursor

  protected def iterator(state: QueryState,
                         startOffset: Int,
                         endOffset: Int,
                         baseContext: CypherRow,
                         cursor: RelationshipValueIndexCursor): IndexIteratorBase[CypherRow]
}

trait Directed {
  self: AbstractRelationshipIndexStringScanSlottedPipe =>
  override protected def iterator(state: QueryState,
                                  startOffset: Int,
                                  endOffset: Int,
                                  baseContext: CypherRow,
                                  cursor: RelationshipValueIndexCursor): IndexIteratorBase[CypherRow] =
    new SlottedRelationshipIndexIterator(state, startOffset, endOffset, cursor)
}

trait Undirected {
  self: AbstractRelationshipIndexStringScanSlottedPipe =>
  override protected def iterator(state: QueryState,
                                  startOffset: Int,
                                  endOffset: Int,
                                  baseContext: CypherRow,
                                  cursor: RelationshipValueIndexCursor): IndexIteratorBase[CypherRow] =
    new SlottedUndirectedRelationshipIndexIterator(state, startOffset, endOffset, cursor)
}

case class DirectedRelationshipIndexContainsScanSlottedPipe(ident: String,
                                                          startNode: String,
                                                          endNode: String,
                                                          property: SlottedIndexedProperty,
                                                          queryIndexId: Int,
                                                          valueExpr: Expression,
                                                          slots: SlotConfiguration,
                                                          indexOrder: IndexOrder)
                                                         (val id: Id = Id.INVALID_ID)
  extends AbstractRelationshipIndexStringScanSlottedPipe(ident, startNode, endNode, property, queryIndexId, valueExpr, slots) with Directed {
  override protected def queryContextCall(state: QueryState,
                                          index: IndexReadSession,
                                          value: TextValue): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByContains(index, needsValues, indexOrder, value)
}

case class UndirectedRelationshipIndexContainsScanSlottedPipe(ident: String,
                                                          startNode: String,
                                                          endNode: String,
                                                          property: SlottedIndexedProperty,
                                                          queryIndexId: Int,
                                                          valueExpr: Expression,
                                                          slots: SlotConfiguration,
                                                          indexOrder: IndexOrder)
                                                         (val id: Id = Id.INVALID_ID)
  extends AbstractRelationshipIndexStringScanSlottedPipe(ident, startNode, endNode, property, queryIndexId, valueExpr, slots) with Undirected {
  override protected def queryContextCall(state: QueryState,
                                          index: IndexReadSession,
                                          value: TextValue): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByContains(index, needsValues, indexOrder, value)
}

case class DirectedRelationshipIndexEndsWithScanSlottedPipe(ident: String,
                                                          startNode: String,
                                                          endNode: String,
                                                          property: SlottedIndexedProperty,
                                                          queryIndexId: Int,
                                                          valueExpr: Expression,
                                                          slots: SlotConfiguration,
                                                          indexOrder: IndexOrder)
                                                         (val id: Id = Id.INVALID_ID)
  extends AbstractRelationshipIndexStringScanSlottedPipe(ident, startNode, endNode, property, queryIndexId, valueExpr, slots) with Directed {
  override protected def queryContextCall(state: QueryState,
                                          index: IndexReadSession,
                                          value: TextValue): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value)
}

case class UndirectedRelationshipIndexEndsWithScanSlottedPipe(ident: String,
                                                            startNode: String,
                                                            endNode: String,
                                                            property: SlottedIndexedProperty,
                                                            queryIndexId: Int,
                                                            valueExpr: Expression,
                                                            slots: SlotConfiguration,
                                                            indexOrder: IndexOrder)
                                                           (val id: Id = Id.INVALID_ID)
  extends AbstractRelationshipIndexStringScanSlottedPipe(ident, startNode, endNode, property, queryIndexId, valueExpr, slots) with Undirected {
  override protected def queryContextCall(state: QueryState,
                                          index: IndexReadSession,
                                          value: TextValue): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value)
}




