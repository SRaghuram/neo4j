/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.ValueConversion.asValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.Value
import org.scalatest.mock.MockitoSugar

/**
  * @param data The keys must be either a String or an ASTCachedProperty. Using Any for convenience in the test classes.
  */
case class FakeSlottedPipe(data: Iterable[Map[Any, Any]],
                           slots: SlotConfiguration)
  extends Pipe with MockitoSugar {

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    data.iterator.map { values =>
      val result = SlottedExecutionContext(slots)

      values foreach {
        case (key: String, value) =>
          slots(key) match {
            case LongSlot(offset, _, _) if value == null =>
              result.setLongAt(offset, -1)

            case LongSlot(offset, _, _) =>
              result.setLongAt(offset, value.asInstanceOf[Number].longValue())

            case RefSlot(offset, _, _) =>
              result.setRefAt(offset, asValue(value))
          }
        case (cachedProp: ASTCachedProperty, value) =>
          slots.getCachedPropertySlot(cachedProp).foreach(refSlot =>result.setCachedPropertyAt(refSlot.offset, asValue(value).asInstanceOf[Value]))
      }
      result
    }
  }

  var id: Id = Id.INVALID_ID
}
