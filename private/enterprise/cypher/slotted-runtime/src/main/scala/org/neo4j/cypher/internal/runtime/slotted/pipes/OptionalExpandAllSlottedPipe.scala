/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPropertyKeys
import org.neo4j.cypher.internal.runtime.slotted.pipes.ExpandAllSlottedPipe.cacheNodeProperties
import org.neo4j.cypher.internal.runtime.slotted.pipes.ExpandAllSlottedPipe.cacheRelationshipProperties
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections
import org.neo4j.values.storable.Values

abstract class OptionalExpandAllSlottedPipe(source: Pipe,
                                            fromSlot: Slot,
                                            relOffset: Int,
                                            toOffset: Int,
                                            dir: SemanticDirection,
                                            types: RelationshipTypes,
                                            slots: SlotConfiguration,
                                            nodePropsToRead: Option[SlottedPropertyKeys],
                                            relsPropsToRead: Option[SlottedPropertyKeys])
  extends PipeWithSource(source) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {
    input.flatMap {
      inputRow: CypherRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)

        if (NullChecker.entityIsNull(fromNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val nodeCursor = state.query.nodeCursor()
          val relCursor = state.query.traversalCursor()
          try {
            val read = state.query.transactionalContext.dataRead
            read.singleNode(fromNode, nodeCursor)
            if (!nodeCursor.next()) {
              Iterator.empty
            } else {
              val nodePropsToCache = ExpandAllSlottedPipe.getNodePropertiesToCache(nodePropsToRead, nodeCursor, state.cursors.propertyCursor, state.query)
              val selectionCursor = dir match {
                case OUTGOING => RelationshipSelections.outgoingCursor(relCursor, nodeCursor, types.types(state.query))
                case INCOMING => RelationshipSelections.incomingCursor(relCursor, nodeCursor, types.types(state.query))
                case BOTH => RelationshipSelections.allCursor(relCursor, nodeCursor, types.types(state.query))
              }
              val matchIterator = filter(new ExpandIterator(selectionCursor, state.query) {
                override protected def createOutputRow(relationship: Long, otherNode: Long): SlottedRow = {
                  val outputRow = SlottedRow(slots)
                  outputRow.copyAllFrom(inputRow)
                  outputRow.setLongAt(relOffset, relationship)
                  outputRow.setLongAt(toOffset, otherNode)
                  cacheNodeProperties(nodePropsToCache, outputRow)
                  cacheRelationshipProperties(relsPropsToRead, relCursor, state.cursors.propertyCursor, outputRow, state.query)
                  outputRow
                }
              }, state)

              if (matchIterator.isEmpty)
                Iterator(withNulls(inputRow))
              else
                matchIterator
            }
          } finally {
            nodeCursor.close()
          }
        }
    }
  }
  def filter(iterator: Iterator[SlottedRow], state: QueryState): Iterator[SlottedRow]

  private def withNulls(inputRow: CypherRow) = {
    val outputRow = SlottedRow(slots)
    outputRow.copyAllFrom(inputRow)
    outputRow.setLongAt(relOffset, -1)
    outputRow.setLongAt(toOffset, -1)
    outputRow
  }

}

object OptionalExpandAllSlottedPipe {

  def apply(source: Pipe,
            fromSlot: Slot,
            relOffset: Int,
            toOffset: Int,
            dir: SemanticDirection, types: RelationshipTypes,
            slots: SlotConfiguration,
            maybePredicate: Option[Expression],
            nodePropsToRead: Option[SlottedPropertyKeys] = None,
            relPropsToRead: Option[SlottedPropertyKeys] = None)
           (id: Id = Id.INVALID_ID): OptionalExpandAllSlottedPipe = maybePredicate match {
    case Some(predicate) => FilteringOptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, types,
      slots, predicate, nodePropsToRead, relPropsToRead)(id)
    case None => NonFilteringOptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, types, slots, nodePropsToRead, relPropsToRead)(id)
  }
}

case class NonFilteringOptionalExpandAllSlottedPipe(source: Pipe,
                                                    fromSlot: Slot,
                                                    relOffset: Int,
                                                    toOffset: Int,
                                                    dir: SemanticDirection,
                                                    types: RelationshipTypes,
                                                    slots: SlotConfiguration,
                                                    nodePropsToRead: Option[SlottedPropertyKeys] = None,
                                                    relsPropsToRead: Option[SlottedPropertyKeys] = None)(val id: Id)
  extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots, nodePropsToRead, relsPropsToRead) {

  override def filter(iterator: Iterator[SlottedRow], state: QueryState): Iterator[SlottedRow] = iterator
}

case class FilteringOptionalExpandAllSlottedPipe(source: Pipe,
                                                 fromSlot: Slot,
                                                 relOffset: Int,
                                                 toOffset: Int,
                                                 dir: SemanticDirection,
                                                 types: RelationshipTypes,
                                                 slots: SlotConfiguration,
                                                 predicate: Expression,
                                                 nodePropsToRead: Option[SlottedPropertyKeys] = None,
                                                 relsPropsToRead: Option[SlottedPropertyKeys] = None)(val id: Id)
  extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots, nodePropsToRead, relsPropsToRead) {

  override def filter(iterator: Iterator[SlottedRow], state: QueryState): Iterator[SlottedRow] =
    iterator.filter(ctx => predicate(ctx, state) eq Values.TRUE)
}
