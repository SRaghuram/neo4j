/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

abstract class AbstractArgumentStateMapTest extends CypherFunSuite {
  private val argumentId = 0L
  def argumentStateMap: ArgumentStateMap[TestArgumentState]

  test("should not be able to take when count not zero") {
    val asm = argumentStateMap
    asm.initiate(argumentId, mock[MorselReadCursor], Array.emptyLongArray, 1)
    asm.takeCompletedExclusive(argumentId) shouldBe null
    asm.decrement(argumentId)
    asm.takeCompletedExclusive(argumentId) should not be null
  }

  test("should not be able to take when peek count not zero") {
    val asm = argumentStateMap
    asm.initiate(argumentId, mock[MorselReadCursor], Array.emptyLongArray, 0)
    asm.trackedPeek(argumentId)
    asm.takeCompletedExclusive(argumentId) shouldBe null
    asm.unTrackPeek(argumentId)
    asm.takeCompletedExclusive(argumentId) should not be null
  }
}

class ConcurrentSingletonArgumentStateMapTest extends AbstractArgumentStateMapTest {
  override def argumentStateMap = new ConcurrentSingletonArgumentStateMap(ArgumentStateMapId(0), TestArgumentStateFactory)
}

class ConcurrentArgumentStateMapTest extends AbstractArgumentStateMapTest {
  override def argumentStateMap = new ConcurrentArgumentStateMap(ArgumentStateMapId(0), 0, TestArgumentStateFactory)
}