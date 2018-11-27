/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

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
                                 lazyTypes: LazyTypes,
                                 slots: SlotConfiguration)
                                (val id: Id = Id.INVALID_ID)
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
      inputRow =>
        val fromNode = getFromNodeFunction(inputRow)
        val toNode = getToNodeFunction(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode))
          Iterator.empty
        else {
          val relationships: LongIterator = relCache.get(fromNode, toNode, dir)
            .getOrElse(findRelationships(state.query, fromNode, toNode, relCache, dir, lazyTypes.types(state.query)))

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
