/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

trait SlottedPipeTestHelper extends CypherFunSuite {

  def testableResult(list: Iterator[ExecutionContext], slots: SlotConfiguration): List[Map[String, Any]] = {
    val list1 = list.toList
    list1 map { in =>
      val build = scala.collection.mutable.HashMap.empty[String, Any]
      slots.foreachSlot({
        case (column, LongSlot(offset, _, _)) => build.put(column, in.getLongAt(offset))
        case (column, RefSlot(offset, _, _)) => build.put(column, in.getRefAt(offset))
      }, cachedNodeProp => null)
      build.toMap
    }
  }
}
