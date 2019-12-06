/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrimitiveLongHelper}
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto

/**
  * Expand when both end-points are known, find all relationships of the given
  * type in the given direction between the two end-points.
  *
  * This is done by checking both nodes and starts from any non-dense node of the two.
  * If both nodes are dense, we find the degree of each and expand from the smaller of the two
  *
  * This pipe also caches relationship information between nodes for the duration of the query
  */
case class ExpandIntoSlottedPipe(source: Pipe,
                                 fromSlot: Slot,
                                 relOffset: Int,
                                 toSlot: Slot,
                                 dir: SemanticDirection,
                                 lazyTypes: RelationshipTypes,
                                 slots: SlotConfiguration)
                                (val id: Id = Id.INVALID_ID)
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
    val expandInto = new CachingExpandInto(query.transactionalContext.dataRead, kernelDirection)
    val nodeCursor = query.nodeCursor()
    input.flatMap {
      inputRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        val toNode = getToNodeFunction.applyAsLong(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode))
          Iterator.empty
        else {
          val groupCursor = query.groupCursor()
          val traversalCursor = query.traversalCursor()
          val relationships =
            new RelationshipCursorIterator(expandInto.connectingRelationships(nodeCursor,
                                                                              groupCursor,
                                                                              traversalCursor,
                                                                              fromNode,
                                                                              lazyTypes.types(query),
                                                                              toNode))
          PrimitiveLongHelper.map(relationships, (relId: Long) => {
            val outputRow = SlottedExecutionContext(slots)
            inputRow.copyTo(outputRow)
            outputRow.setLongAt(relOffset, relId)
            outputRow
          })
        }
    }
  }
}
