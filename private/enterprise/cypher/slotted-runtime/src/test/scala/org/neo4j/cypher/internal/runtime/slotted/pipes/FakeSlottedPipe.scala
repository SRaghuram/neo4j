/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.ValueConversion.asValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.scalatest.mock.MockitoSugar

case class FakeSlottedPipe(data: Iterable[Map[String, Any]], slots: SlotConfiguration)
  extends Pipe with MockitoSugar {

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    data.iterator.map { values =>
      val result = SlottedExecutionContext(slots)

      values foreach {
        case (key, value) =>
          slots(key) match {
            case LongSlot(offset, _, _) if value == null =>
              result.setLongAt(offset, -1)

            case LongSlot(offset, _, _) =>
              result.setLongAt(offset, value.asInstanceOf[Number].longValue())

            case RefSlot(offset, _, _) =>
              result.setRefAt(offset, asValue(value))
          }
      }
      result
    }
  }

  var id: Id = Id.INVALID_ID
}
