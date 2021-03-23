/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateWithCompleted
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

abstract class AbstractArgumentStateMapTest extends CypherFunSuite {
  private val argumentId = 0L
  def argumentStateMap(withPeekerTracking: Boolean): ArgumentStateMap[TestArgumentState]

  test("should takeIfCompletedOrElsePeek with tracking enabled") {
    val asm = argumentStateMap(true)
    asm.initiate(argumentId, mock[MorselReadCursor], Array.emptyLongArray, initialCount = 1) // complete = false (count = 1, peekers = 0)
    asm.takeIfCompletedOrElseTrackedPeek(argumentId) shouldBe ArgumentStateWithCompleted[TestArgumentState](asm.peek(argumentId), isCompleted = false) // complete = false (count = 1, peekers = 1)
    asm.decrement(argumentId)
    asm.takeIfCompletedOrElseTrackedPeek(argumentId) shouldBe null // complete = false (count = 0, peekers = 1)
    asm.untrackPeek(argumentId)
    val state = asm.peek(argumentId)
    asm.takeIfCompletedOrElseTrackedPeek(argumentId) shouldBe ArgumentStateWithCompleted[TestArgumentState](state, isCompleted = true) // complete = true (count = 0, peekers = 0)
  }
}

class ConcurrentSingletonArgumentStateMapTest extends AbstractArgumentStateMapTest {
  override def argumentStateMap(withPeekerTracking: Boolean) = new ConcurrentSingletonArgumentStateMap(ArgumentStateMapId(0), TestArgumentStateFactory(withPeekerTracking))
}

class ConcurrentArgumentStateMapTest extends AbstractArgumentStateMapTest {
  override def argumentStateMap(withPeekerTracking: Boolean) = new ConcurrentArgumentStateMap(ArgumentStateMapId(0), 0, TestArgumentStateFactory(withPeekerTracking))
}