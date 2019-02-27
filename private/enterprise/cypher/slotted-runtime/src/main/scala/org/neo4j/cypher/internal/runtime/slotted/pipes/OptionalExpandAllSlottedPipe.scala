/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrimitiveLongHelper, RelationshipIterator}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.Values

abstract class OptionalExpandAllSlottedPipe(source: Pipe,
                                            fromSlot: Slot,
                                            relOffset: Int,
                                            toOffset: Int,
                                            dir: SemanticDirection,
                                            types: LazyTypes,
                                            slots: SlotConfiguration)
  extends PipeWithSource(source) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      inputRow: ExecutionContext =>
        val fromNode = getFromNodeFunction(inputRow)

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
            val outputRow = SlottedExecutionContext(slots)
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

  def filter(iterator: Iterator[SlottedExecutionContext], state: QueryState): Iterator[SlottedExecutionContext]

  private def withNulls(inputRow: ExecutionContext) = {
    val outputRow = SlottedExecutionContext(slots)
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
            dir: SemanticDirection, types: LazyTypes,
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
                                               types: LazyTypes,
                                               slots: SlotConfiguration)(val id: Id)
  extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots) {

  override def filter(iterator: Iterator[SlottedExecutionContext], state: QueryState): Iterator[SlottedExecutionContext] = iterator
}

case class FilteringOptionalExpandAllSlottedPipe(source: Pipe,
                                            fromSlot: Slot,
                                            relOffset: Int,
                                            toOffset: Int,
                                            dir: SemanticDirection,
                                            types: LazyTypes,
                                            slots: SlotConfiguration,
                                            predicate: Expression)(val id: Id)
  extends OptionalExpandAllSlottedPipe(source: Pipe, fromSlot, relOffset, toOffset, dir, types, slots) {

  override def filter(iterator: Iterator[SlottedExecutionContext], state: QueryState): Iterator[SlottedExecutionContext] =
    iterator.filter(ctx => predicate(ctx, state) eq Values.TRUE)
}