/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class UndirectedRelationshipIndexScanSlottedPipe(ident: String,
                                                      startNode: String,
                                                      endNode: String,
                                                      relType: RelationshipTypeToken,
                                                      properties: IndexedSeq[SlottedIndexedProperty],
                                                      queryIndexId: Int,
                                                      indexOrder: IndexOrder,
                                                      slots: SlotConfiguration)
                                                     (val id: Id = Id.INVALID_ID) extends Pipe with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  override val indexPropertySlotOffsets: Array[Int] = properties.map(_.maybeCachedNodePropertySlot).collect{ case Some(o) => o }.toArray
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val cursor = state.query.relationshipIndexScan(state.queryIndexes(queryIndexId), needsValues, indexOrder)

    new SlottedUndirectedRelationshipIndexIterator(state, slots.getLongOffsetFor(startNode), slots.getLongOffsetFor(endNode), cursor)
  }
}
