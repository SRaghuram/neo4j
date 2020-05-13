/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.pipelined.SchedulingResult
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardStateFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LowMarkSchedulingTest extends CypherFunSuite {

  test("should schedule non-prioritized items correctly") {
    // given
    val x = new TestScheduling(Array(false, false, false, false))

    // then
    x.schedule(3) shouldBe "3"
    x.schedule(3) shouldBe "3"
    x.schedule(2) shouldBe "2"
    x.schedule(2) shouldBe "2"
    x.schedule(1) shouldBe "1"
    x.schedule(0) shouldBe "0"

    x.scheduleAttempts should be < (x.scheduleCalls * x.numPipelines)
  }

  test("should schedule recurring items correctly") {
    // given
    val x = new TestScheduling(Array(false, false, false, false))

    // then
    x.schedule(3) shouldBe "3"
    x.schedule(2) shouldBe "2"
    x.schedule(3) shouldBe "3"
    x.schedule(2) shouldBe "2"
    x.schedule(1) shouldBe "1"
    x.schedule(0) shouldBe "0"

    x.scheduleAttempts should be < (x.scheduleCalls * x.numPipelines)
  }

  test("should schedule perfect sequence perfectly") {
    // given
    val x = new TestScheduling(Array(false, false, false, false))

    // then
    x.schedule(3) shouldBe "3"
    x.schedule(2) shouldBe "2"
    x.schedule(1) shouldBe "1"
    x.schedule(0) shouldBe "0"

    x.scheduleAttempts shouldBe x.scheduleCalls
  }

  test("should schedule prioritized items correctly") {
    // given
    val x = new TestScheduling(Array(true, true, true, true))

    // then
    x.schedule(3) shouldBe "3"
    x.schedule(3) shouldBe "3"
    x.schedule(2) shouldBe "2"
    x.schedule(2) shouldBe "2"
    x.schedule(1) shouldBe "1"
    x.schedule(0) shouldBe "0"

    x.scheduleAttempts should be < (x.scheduleCalls * x.numPipelines)
  }

  test("should schedule prioritized items out of order") {
    // given
    val x = new TestScheduling(Array(false, true, false, true, false, false))

    // then
    x.schedule(5) shouldBe "5"
    x.schedule(5) shouldBe "5"
    x.schedule(3) shouldBe "3"
    x.schedule(1) shouldBe "1"
    x.schedule(0) shouldBe "0"

    x.scheduleAttempts should be < (x.scheduleCalls * x.numPipelines)
  }

  test("should have small overhead for prioritized item") {
    // given
    val x = new TestScheduling(Array(false, true, false, false, false, false))

    // then
    x.schedule(5) shouldBe "5" // extra attempt for prio item
    x.schedule(1) shouldBe "1"
    x.schedule(0) shouldBe "0"

    x.scheduleAttempts shouldBe 4
  }

  class TestScheduling(priority: Array[Boolean]) extends LowMarkScheduling[String](new StandardStateFactory, priority) {

    var scheduleCalls: Int = 0
    var scheduleAttempts: Int = 0
    val numPipelines: Int = priority.length

    private var availableItem: Int = 0
    def schedule(availableItem: Int): String = {
      this.availableItem = availableItem
      this.scheduleCalls += 1
      schedule(null).task
    }

    override protected def tryScheduleItem(item: Int,
                                           queryResources: QueryResources): SchedulingResult[String] = {
      scheduleAttempts += 1
      SchedulingResult(
        if (item == availableItem) {
          item.toString
        } else {
          null
        }, someTaskWasFilteredOut = false)
    }
  }
}
