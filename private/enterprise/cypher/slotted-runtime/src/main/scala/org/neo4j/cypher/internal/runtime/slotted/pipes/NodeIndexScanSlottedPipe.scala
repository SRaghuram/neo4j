/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexOrder
import org.neo4j.internal.kernel.api.IndexReference
import org.neo4j.cypher.internal.v3_5.expressions.LabelToken
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

case class NodeIndexScanSlottedPipe(ident: String,
                                    label: LabelToken,
                                    property: SlottedIndexedProperty,
                                    indexOrder: IndexOrder,
                                    slots: SlotConfiguration,
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID)
  extends Pipe with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val indexPropertySlotOffsets: Array[Int] = property.maybeCachedNodePropertySlot.toArray
  override val indexPropertyIndices: Array[Int] = indexPropertySlotOffsets.map(_ => 0)
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, property.propertyKeyId)
    }
    reference
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val cursor = state.query.indexScan(reference(state.query), needsValues, indexOrder)
    new SlottedIndexIterator(state, slots, cursor)
  }
}
