/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class StandardQueryCompletionTrackerTest extends QueryCompletionTrackerTest {
  override def newTracker(): QueryCompletionTracker = new StandardQueryCompletionTracker
}

class ConcurrentQueryCompletionTrackerTest extends QueryCompletionTrackerTest {
  override def newTracker(): QueryCompletionTracker = new StandardQueryCompletionTracker
}

abstract class QueryCompletionTrackerTest extends CypherFunSuite {

  def newTracker(): QueryCompletionTracker

  test("await should return normally if query has completed") {
    val x = newTracker()

    // when
    x.increment()
    x.decrement()

    // then
    x.await()
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
    x.error(new IllegalArgumentException)

    // then
    x.isCompleted should be(true)
  }

}
