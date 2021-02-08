/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EntityIndexSeeker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class DirectedRelationshipIndexSeekSlottedPipe(ident: String,
                                                    startNode: String,
                                                    endNode: String,
                                                    relType: RelationshipTypeToken,
                                                    properties: IndexedSeq[SlottedIndexedProperty],
                                                    queryIndexId: Int,
                                                    valueExpr: QueryExpression[Expression],
                                                    indexMode: IndexSeekMode = IndexSeek,
                                                    indexOrder: IndexOrder,
                                                    slots: SlotConfiguration)
                                                   (val id: Id = Id.INVALID_ID) extends Pipe with EntityIndexSeeker with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId).toArray

  override val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  override val indexPropertySlotOffsets: Array[Int] = properties.map(_.maybeCachedNodePropertySlot).collect{ case Some(o) => o }.toArray
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val index = state.queryIndexes(queryIndexId)
    val context = state.newRowWithArgument(rowFactory)
    new SlottedRelationshipIndexIterator(state, slots.getLongOffsetFor(startNode), slots.getLongOffsetFor(endNode), relationshipIndexSeek(state, index, needsValues, indexOrder, context))
  }
}
