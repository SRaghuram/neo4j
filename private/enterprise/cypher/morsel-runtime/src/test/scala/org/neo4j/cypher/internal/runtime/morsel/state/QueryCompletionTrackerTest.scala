/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.QuerySubscriber

class StandardQueryCompletionTrackerTest extends QueryCompletionTrackerTest {
  override def newTracker(): QueryCompletionTracker = new StandardQueryCompletionTracker(mock[QuerySubscriber],
                                                                                         mock[QueryContext](RETURNS_DEEP_STUBS),
                                                                                         mock[QueryExecutionTracer])
}

class ConcurrentQueryCompletionTrackerTest extends QueryCompletionTrackerTest {
  override def newTracker(): QueryCompletionTracker = new ConcurrentQueryCompletionTracker(mock[QuerySubscriber],
                                                                                         mock[QueryContext](RETURNS_DEEP_STUBS),
                                                                                         mock[QueryExecutionTracer])
}

abstract class QueryCompletionTrackerTest extends CypherFunSuite {

  def newTracker(): QueryCompletionTracker

  test("await should return normally if query has completed") {
    val x = newTracker()

    // when
    x.increment()
    x.decrement()

    // then
    x.await() shouldBe false
  }

  test("await should return normally if query has been cancelled") {
    val x = newTracker()

    // when
    x.increment()
    x.cancel()

    // then
    x.await() shouldBe false
  }

  test("await should return normally if query has no demand") {
    val x = newTracker()

    // when
    x.increment()
    x.request(1)
    x.addServed(1)

    // then
    x.await() shouldBe true
  }

  test("await should throw if query has failed") {
    val x = newTracker()

    // when
    x.error(new IllegalArgumentException)

    // then
    intercept[IllegalArgumentException] {
      x.await()
    }
  }

  test("isCompleted when not completed") {
    val x = newTracker()

    // when
    x.increment()

    // then
    x.isCompleted should be(false)
  }

  test("isCompleted when completed") {
    val x = newTracker()

    // when
    x.increment()
    x.decrement()

    // then
    x.isCompleted should be(true)
  }

  test("isCompleted when error") {
    val x = newTracker()

    // when
    x.increment()
    x.error(new IllegalArgumentException)

    // then
    x.isCompleted should be(true)
  }

  test("isCompleted when cancelled") {
    val x = newTracker()

    // when
    x.increment()
    x.cancel()

    // then
    x.isCompleted should be(true)
  }

}
