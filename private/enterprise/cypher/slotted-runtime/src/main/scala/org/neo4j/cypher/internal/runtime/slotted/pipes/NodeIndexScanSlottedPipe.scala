/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class NodeIndexScanSlottedPipe(ident: String,
                                    label: LabelToken,
                                    properties: Seq[SlottedIndexedProperty],
                                    queryIndexId: Int,
                                    indexOrder: IndexOrder,
                                    slots: SlotConfiguration)
                                   (val id: Id = Id.INVALID_ID)
  extends Pipe with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot).toArray
  override val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.maybeCachedNodePropertySlot.isDefined).map(_._2).toArray
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val cursor = state.query.nodeIndexScan(state.queryIndexes(queryIndexId), needsValues, indexOrder)
    new SlottedNodeIndexIterator(state, cursor)
  }
}
