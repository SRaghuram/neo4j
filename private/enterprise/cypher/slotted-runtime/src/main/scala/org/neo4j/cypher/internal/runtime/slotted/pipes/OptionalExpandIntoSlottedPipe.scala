/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrimitiveLongHelper}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.graphdb.Direction
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
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val query = state.query
    val expandInto = new org.neo4j.internal.kernel.api.helpers.CachingExpandInto(query.transactionalContext.dataRead,
                                                                                 kernelDirection,
                                                                                 lazyTypes.types(query))
    val nodeCursor = query.nodeCursor()
    input.flatMap {
      inputRow: ExecutionContext =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        val toNode = getToNodeFunction.applyAsLong(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val groupCursor = query.groupCursor()
          val traversalCursor = query.traversalCursor()
          val relationships =
            new RelationshipCursorIterator(expandInto.connectingRelationships(nodeCursor,
                                                                              groupCursor,
                                                                              traversalCursor,
                                                                              fromNode,
                                                                              toNode))
          val matchIterator = findMatchIterator(inputRow, state, relationships)

          if (matchIterator.isEmpty) Iterator(withNulls(inputRow))
          else matchIterator
        }
    }
  }

  def findMatchIterator(inputRow: ExecutionContext,
                        state: QueryState,
                        relationships: LongIterator): Iterator[SlottedExecutionContext]

  private def withNulls(inputRow: ExecutionContext) = {
    val outputRow = SlottedExecutionContext(slots)
    inputRow.copyTo(outputRow)
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

  override def findMatchIterator(inputRow: ExecutionContext,
                                 state: QueryState,
                                 relationships: LongIterator): Iterator[SlottedExecutionContext] = {
    PrimitiveLongHelper.map(relationships, relId => {
      val outputRow = SlottedExecutionContext(slots)
      inputRow.copyTo(outputRow)
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

  predicate.registerOwningPipe(this)

  override def findMatchIterator(inputRow: ExecutionContext,
                                 state: QueryState,
                                 relationships: LongIterator): Iterator[SlottedExecutionContext] = {
    PrimitiveLongHelper.map(relationships, relId => {
      val outputRow = SlottedExecutionContext(slots)
      inputRow.copyTo(outputRow)
      outputRow.setLongAt(relOffset, relId)
      outputRow
    }).filter(ctx => predicate(ctx, state) eq Values.TRUE)
  }
}
