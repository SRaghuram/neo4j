/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.logical.plans.{IndexOrder, QueryExpression}
import org.opencypher.v9_0.expressions.LabelToken
import org.opencypher.v9_0.util.attribution.Id

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

  valueExpr.expressions.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val index = state.queryIndexes(queryIndexId)
    val contextForIndexExpression = state.initialContext.getOrElse(SlottedExecutionContext.empty)
    indexSeek(state, index, needsValues, indexOrder, contextForIndexExpression).flatMap(
      cursor => new SlottedIndexIterator(state, slots, cursor)
    )
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
