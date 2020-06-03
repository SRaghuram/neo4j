/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Value
import org.scalatest.mockito.MockitoSugar

/**
 * @param data The keys must be either a String or an ASTCachedProperty. Using Any for convenience in the test classes.
 */
case class FakeSlottedPipe(data: Iterable[Map[Any, Any]],
                           slots: SlotConfiguration)
  extends Pipe with MockitoSugar {

  def internalCreateResults(state: QueryState): Iterator[CypherRow] = {
    data.iterator.map { values =>
      val result = SlottedRow(slots)

      values foreach {
        case (key: String, value) =>
          slots(key) match {
            case LongSlot(offset, _, _) if value == null =>
              result.setLongAt(offset, -1)

            case LongSlot(offset, _, _) =>
              result.setLongAt(offset, value.asInstanceOf[Number].longValue())

            case RefSlot(offset, _, _) =>
              result.setRefAt(offset, ValueUtils.of(value))
          }
        case (cachedProp: ASTCachedProperty, value) =>
          slots.getCachedPropertySlot(cachedProp).foreach(refSlot =>result.setCachedPropertyAt(refSlot.offset, ValueUtils.of(value).asInstanceOf[Value]))
      }
      result
    }
  }

  var id: Id = Id.INVALID_ID
}
