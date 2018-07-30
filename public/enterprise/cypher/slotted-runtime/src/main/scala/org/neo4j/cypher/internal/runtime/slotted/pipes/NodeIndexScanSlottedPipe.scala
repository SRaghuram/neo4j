/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.internal.kernel.api.IndexReference
import org.opencypher.v9_0.expressions.{LabelToken, PropertyKeyToken}
import org.opencypher.v9_0.util.attribution.Id

case class NodeIndexScanSlottedPipe(ident: String,
                                    label: LabelToken,
                                    propertyKey: PropertyKeyToken,
                                    getValueFromIndex: Boolean,
                                    slots: SlotConfiguration,
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID)
  extends Pipe with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val propertyIndicesWithValues: Seq[Int] = if (getValueFromIndex) Seq(0) else Seq.empty
  val propertyNamesWithValues: Seq[String] = if (getValueFromIndex) Seq(ident + "." + propertyKey.name) else Seq.empty
  override val propertyOffsets: Seq[Int] = propertyNamesWithValues.map(name => slots.getReferenceOffsetFor(name))

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id,propertyKey.nameId.id)
    }
    reference
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    if (propertyIndicesWithValues.isEmpty) {
      val nodes = state.query.indexScanPrimitive(reference(state.query))
      PrimitiveLongHelper.map(nodes, { node =>

        val context = SlottedExecutionContext(slots)
        state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
        context.setLongAt(offset, node)
        context
      })
    } else {
      val results = state.query.indexScanPrimitiveWithValues(reference(state.query), propertyIndicesWithValues)
      createResultsFromPrimitiveTupleIterator(state, slots, results)
    }
  }

}
