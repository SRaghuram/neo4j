/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadLocalRandom}

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.scheduling.QueryExecutionTracer
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.scalatest.concurrent.Eventually

class ConcurrentQueryCompletionTrackerStressTest extends CypherFunSuite {

  private val SIZE = 1000

  test("should handle concurrent access") {
    // Given
    val subscriber = new Subscriber
    val tracker = new ConcurrentQueryCompletionTracker(subscriber,
                                                       mock[QueryContext](RETURNS_DEEP_STUBS),
                                                       mock[QueryExecutionTracer])

    reset(tracker)


    // When
    val executor = Executors.newFixedThreadPool(2)
    executor.submit(new DemandServingThread(tracker))
    executor.submit(new DecrementThread(tracker))
    val random = ThreadLocalRandom.current()

    // Then
    1 to SIZE foreach { _ =>
      val request = random.nextLong(SIZE)
      tracker.request(request)

      //if tracker.await returns false there is no more data then we know that we eventually should
      //call onComplete on the subscriber. However if tracker says it has more data we cannot be completely
      //sure it will not finish soon after.
      if (!tracker.await()) {
        tracker.isCompleted shouldBe true
        Eventually.eventually {
          subscriber.done.get() should equal(true)
          subscriber.done.set(false)
        }
        reset(tracker)
      }
    }

    executor.shutdown()
  }

  private def reset(tracker: ConcurrentQueryCompletionTracker): Unit =  {
    1 to SIZE foreach { _ =>
      tracker.increment()
    }
  }
}

class DecrementThread(tracker: ConcurrentQueryCompletionTracker) extends Runnable {

  override def run(): Unit = {
    while (true) {
      if (!tracker.isCompleted && tracker.hasDemand) {
        tracker.decrement()
      }
    }
  }
}

class DemandServingThread(tracker: ConcurrentQueryCompletionTracker) extends Runnable {
  override def run(): Unit = {
    while (true) {
      if (!tracker.isCompleted && tracker.hasDemand)
        tracker.addServed(1)
    }
  }
}

class Subscriber extends QuerySubscriber {
  val done = new AtomicBoolean(false)
  override def onResult(numberOfFields: Int): Unit = {}
  override def onRecord(): Unit = {}
  override def onField(offset: Int, value: AnyValue): Unit = {}
  override def onRecordCompleted(): Unit = {}
  override def onError(throwable: Throwable): Unit = {}
  override def onResultCompleted(statistics: QueryStatistics): Unit = {
    done.set(true)
  }
}
