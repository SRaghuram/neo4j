/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.Values

abstract class OptionalExpandAllSlottedPipe(source: Pipe,
                                            fromSlot: Slot,
                                            relOffset: Int,
                                            toOffset: Int,
                                            dir: SemanticDirection,
                                            types: RelationshipTypes,
                                            slots: SlotConfiguration)
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
          val relationships: RelationshipIterator = state.query.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(state.query))
          var otherSide: Long = 0

          val relVisitor = new RelationshipVisitor[InternalException] {
            override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit =
              if (fromNode == startNodeId)
                otherSide = endNodeId
              else
                otherSide = startNodeId
          }

          val matchIterator = filter(PrimitiveLongHelper.map(relationships, relId => {
            relationships.relationshipVisit(relId, relVisitor)
            val outputRow = SlottedRow(slots)
            inputRow.copyTo(outputRow)
            outputRow.setLongAt(relOffset, relId)
            outputRow.setLongAt(toOffset, otherSide)
            outputRow
          }), state)

          if (matchIterator.isEmpty)
            Iterator(withNulls(inputRow))
          else
            matchIterator
        }
    }
  }

  def filter(iterator: Iterator[SlottedRow], state: QueryState): Iterator[SlottedRow]

  private def withNulls(inputRow: CypherRow) = {
    val outputRow = SlottedRow(slots)
    inputRow.copyTo(outputRow)
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
            maybePredicate: Option[Expression])
           (id: Id = Id.INVALID_ID): OptionalExpandAllSlottedPipe = maybePredicate match {
    case Some(predicate) => FilteringOptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, types,
      slots, predicate)(id)
    case None => NonFilteringOptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, types, slots)(id)
  }
}

case class NonFilteringOptionalExpandAllSlottedPipe(source: Pipe,
                                                    fromSlot: Slot,
                                                    relOffset: Int,
                                                    toOffset: Int,
                                                    dir: SemanticDirection,
                                                    types: RelationshipTypes,
                                                    slots: SlotConfiguration)(val id: Id)
  extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots) {

  override def filter(iterator: Iterator[SlottedRow], state: QueryState): Iterator[SlottedRow] = iterator
}

case class FilteringOptionalExpandAllSlottedPipe(source: Pipe,
                                                 fromSlot: Slot,
                                                 relOffset: Int,
                                                 toOffset: Int,
                                                 dir: SemanticDirection,
                                                 types: RelationshipTypes,
                                                 slots: SlotConfiguration,
                                                 predicate: Expression)(val id: Id)
  extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots) {

  predicate.registerOwningPipe(this)

  override def filter(iterator: Iterator[SlottedRow], state: QueryState): Iterator[SlottedRow] =
    iterator.filter(ctx => predicate(ctx, state) eq Values.TRUE)
}
