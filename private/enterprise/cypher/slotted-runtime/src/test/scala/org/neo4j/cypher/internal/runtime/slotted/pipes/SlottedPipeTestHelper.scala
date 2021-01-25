/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait SlottedPipeTestHelper extends CypherFunSuite {

  def testableResult(list: Iterator[CypherRow], slots: SlotConfiguration): List[Map[String, Any]] = {
    val list1 = list.toList
    list1 map { in =>
      val build = scala.collection.mutable.HashMap.empty[String, Any]
      slots.foreachSlotAndAliases({
        case SlotWithKeyAndAliases(VariableSlotKey(column), LongSlot(offset, _, _), aliases) =>
          build.put(column, in.getLongAt(offset))
          aliases.foreach(build.put(_, in.getLongAt(offset)))
        case SlotWithKeyAndAliases(VariableSlotKey(column), RefSlot(offset, _, _), aliases) =>
          build.put(column, in.getRefAt(offset))
          aliases.foreach(build.put(_, in.getRefAt(offset)))
        case _ => // no help here
      })
      build.toMap
    }
  }
}
