/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.values.storable.Values

abstract class OptionalExpandIntoSlottedPipe(source: Pipe,
                                             fromSlot: Slot,
                                             relOffset: Int,
                                             toSlot: Slot,
                                             dir: SemanticDirection,
                                             lazyTypes: RelationshipTypes,
                                             slots: SlotConfiguration)
  extends PipeWithSource(source) {
  self =>

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val kernelDirection = dir match {
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.BOTH => Direction.BOTH
  }
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {
    val query = state.query
    val expandInto = new CachingExpandInto(query.transactionalContext.dataRead, kernelDirection, state.memoryTracker.memoryTrackerForOperator(id.x))
    input.flatMap {
      inputRow: CypherRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        val toNode = getToNodeFunction.applyAsLong(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val traversalCursor = query.traversalCursor()
          val nodeCursor = query.nodeCursor()
          try {
            val selectionCursor = expandInto.connectingRelationships(nodeCursor,
                                                                     traversalCursor,
                                                                     fromNode,
                                                                     lazyTypes.types(query),
                                                                     toNode)
            query.resources.trace(selectionCursor)
            val relationships =
              new RelationshipCursorIterator(selectionCursor)
            val matchIterator = findMatchIterator(inputRow, state, relationships)

            if (matchIterator.isEmpty) Iterator(withNulls(inputRow))
            else matchIterator
          } finally {
            nodeCursor.close()
          }
        }
    }
  }

  def findMatchIterator(inputRow: CypherRow,
                        state: QueryState,
                        relationships: LongIterator): Iterator[SlottedRow]

  private def withNulls(inputRow: CypherRow) = {
    val outputRow = SlottedRow(slots)
    outputRow.copyAllFrom(inputRow)
    outputRow.setLongAt(relOffset, -1)
    outputRow
  }

}

object OptionalExpandIntoSlottedPipe {

  def apply(source: Pipe,
            fromSlot: Slot,
            relOffset: Int,
            toSlot: Slot,
            dir: SemanticDirection,
            lazyTypes: RelationshipTypes,
            slots: SlotConfiguration,
            maybePredicate: Option[Expression])
           (id: Id = Id.INVALID_ID): OptionalExpandIntoSlottedPipe = maybePredicate match {
    case Some(predicate) => FilteringOptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, lazyTypes,
      slots, predicate)(id)
    case None => NonFilteringOptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, lazyTypes, slots)(id)
  }
}

case class NonFilteringOptionalExpandIntoSlottedPipe(source: Pipe,
                                                     fromSlot: Slot,
                                                     relOffset: Int,
                                                     toSlot: Slot,
                                                     dir: SemanticDirection,
                                                     lazyTypes: RelationshipTypes,
                                                     slots: SlotConfiguration)(val id: Id)
  extends OptionalExpandIntoSlottedPipe(source: Pipe, fromSlot, relOffset, toSlot, dir, lazyTypes, slots) {

  override def findMatchIterator(inputRow: CypherRow,
                                 state: QueryState,
                                 relationships: LongIterator): Iterator[SlottedRow] = {
    PrimitiveLongHelper.map(relationships, relId => {
      val outputRow = SlottedRow(slots)
      outputRow.copyAllFrom(inputRow)
      outputRow.setLongAt(relOffset, relId)
      outputRow
    })
  }
}

case class FilteringOptionalExpandIntoSlottedPipe(source: Pipe,
                                                  fromSlot: Slot,
                                                  relOffset: Int,
                                                  toSlot: Slot,
                                                  dir: SemanticDirection,
                                                  lazyTypes: RelationshipTypes,
                                                  slots: SlotConfiguration,
                                                  predicate: Expression)(val id: Id)
  extends OptionalExpandIntoSlottedPipe(source: Pipe, fromSlot, relOffset, toSlot, dir, lazyTypes, slots) {

  override def findMatchIterator(inputRow: CypherRow,
                                 state: QueryState,
                                 relationships: LongIterator): Iterator[SlottedRow] = {
    PrimitiveLongHelper.map(relationships, relId => {
      val outputRow = SlottedRow(slots)
      outputRow.copyAllFrom(inputRow)
      outputRow.setLongAt(relOffset, relId)
      outputRow
    }).filter(ctx => predicate(ctx, state) eq Values.TRUE)
  }
}
