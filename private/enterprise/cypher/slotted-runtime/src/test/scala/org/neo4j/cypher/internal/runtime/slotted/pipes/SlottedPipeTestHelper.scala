/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlot
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait SlottedPipeTestHelper extends CypherFunSuite {

  def testableResult(list: Iterator[ExecutionContext], slots: SlotConfiguration): List[Map[String, Any]] = {
    val list1 = list.toList
    list1 map { in =>
      val build = scala.collection.mutable.HashMap.empty[String, Any]
      slots.foreachSlot({
        case (VariableSlot(column), LongSlot(offset, _, _)) => build.put(column, in.getLongAt(offset))
        case (VariableSlot(column), RefSlot(offset, _, _)) => build.put(column, in.getRefAt(offset))
        case (_: CachedPropertySlot, _) => // no help here
        case (_: ApplyPlanSlot, _) => // or here
      })
      build.toMap
    }
  }
}
