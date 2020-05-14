/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexSeeker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

case class NodeIndexSeekSlottedPipe(ident: String,
                                    label: LabelToken,
                                    properties: IndexedSeq[SlottedIndexedProperty],
                                    queryIndexId: Int,
                                    valueExpr: QueryExpression[Expression],
                                    indexMode: IndexSeekMode = IndexSeek,
                                    indexOrder: IndexOrder,
                                    slots: SlotConfiguration,
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID) extends Pipe with NodeIndexSeeker with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId).toArray

  override val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  override val indexPropertySlotOffsets: Array[Int] = properties.map(_.maybeCachedNodePropertySlot).collect{ case Some(o) => o }.toArray
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): Iterator[CypherRow] = {
    val index = state.queryIndexes(queryIndexId)
    val context = SlottedRow(slots)
    state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
    new SlottedIndexIterator(state, slots, indexSeek(state, index, needsValues, indexOrder, context))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NodeIndexSeekSlottedPipe]

  override def equals(other: Any): Boolean = other match {
    case that: NodeIndexSeekSlottedPipe =>
      (that canEqual this) &&
        ident == that.ident &&
        label == that.label &&
        (properties == that.properties) &&
        valueExpr == that.valueExpr &&
        indexMode == that.indexMode &&
        slots == that.slots &&
        argumentSize == that.argumentSize
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(ident, label, properties, valueExpr, indexMode, slots, argumentSize)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
