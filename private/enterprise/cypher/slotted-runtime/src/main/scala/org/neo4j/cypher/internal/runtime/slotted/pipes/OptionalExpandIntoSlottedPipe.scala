/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrimitiveLongHelper}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.Values

abstract class OptionalExpandIntoSlottedPipe(source: Pipe,
                                             fromSlot: Slot,
                                             relOffset: Int,
                                             toSlot: Slot,
                                             dir: SemanticDirection,
                                             lazyTypes: LazyTypes,
                                             slots: SlotConfiguration,
                                             val id: Id)
  extends PipeWithSource(source) with PrimitiveCachingExpandInto {
  self =>

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private final val CACHE_SIZE = 100000
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new PrimitiveRelationshipsCache(CACHE_SIZE)

    input.flatMap {
      inputRow: ExecutionContext =>
        val fromNode = getFromNodeFunction(inputRow)
        val toNode = getToNodeFunction(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val relationships: LongIterator = relCache.get(fromNode, toNode, dir)
            .getOrElse(findRelationships(state, fromNode, toNode, relCache, dir, lazyTypes.types(state.query)))

          val matchIterator = findMatchIterator(inputRow, state, relationships)

          if (matchIterator.isEmpty)
            Iterator(withNulls(inputRow))
          else
            matchIterator
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
            lazyTypes: LazyTypes,
            slots: SlotConfiguration,
            maybePredicate: Option[Expression])
           (id: Id = Id.INVALID_ID): OptionalExpandIntoSlottedPipe = maybePredicate match {
    case Some(predicate) => new FilteringOptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, lazyTypes,
                                                                      slots, id, predicate)
    case None => new NonFilteringOptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, lazyTypes, slots, id)
  }
}

class NonFilteringOptionalExpandIntoSlottedPipe(source: Pipe,
                                                fromSlot: Slot,
                                                relOffset: Int,
                                                toSlot: Slot,
                                                dir: SemanticDirection,
                                                lazyTypes: LazyTypes,
                                                slots: SlotConfiguration,
                                                id: Id)
  extends OptionalExpandIntoSlottedPipe(source: Pipe, fromSlot, relOffset, toSlot, dir, lazyTypes, slots, id) {

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

class FilteringOptionalExpandIntoSlottedPipe(source: Pipe,
                                             fromSlot: Slot,
                                             relOffset: Int,
                                             toSlot: Slot,
                                             dir: SemanticDirection,
                                             lazyTypes: LazyTypes,
                                             slots: SlotConfiguration,
                                            id: Id,
                                            predicate: Expression)
  extends OptionalExpandIntoSlottedPipe(source: Pipe, fromSlot, relOffset, toSlot, dir, lazyTypes, slots, id) {

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
