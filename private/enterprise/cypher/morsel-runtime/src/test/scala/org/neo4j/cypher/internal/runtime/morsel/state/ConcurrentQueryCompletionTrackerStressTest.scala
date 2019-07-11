/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.{CountDownLatch, Executors, ThreadLocalRandom}

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.QuerySubscriber

class ConcurrentQueryCompletionTrackerStressTest extends CypherFunSuite {

  private val SIZE = 100000
  private val THREADS = 10

  test("should handle concurrent access") {
    val tracker = new ConcurrentQueryCompletionTracker(mock[QuerySubscriber],
      mock[QueryContext](RETURNS_DEEP_STUBS),
      mock[QueryExecutionTracer])
    (1 to SIZE * THREADS) foreach { _ =>
      tracker.increment()
    }

    // When
    val executor = Executors.newFixedThreadPool(THREADS)

    // Then
    val request = SIZE * THREADS
    tracker.request(request)
    val latch = new CountDownLatch(THREADS)

    val threads = (1 to THREADS).map { _ => new DemandServingThread(tracker, request / THREADS, latch) }
    threads.foreach(executor.submit)

    tracker.await() shouldBe false
    tracker.isCompleted shouldBe true

    executor.shutdown()
  }
}

class DemandServingThread(tracker: ConcurrentQueryCompletionTracker,
                          var count: Long,
                          latch: CountDownLatch) extends Runnable {

  private val random = ThreadLocalRandom.current()

  override def run(): Unit = {
    latch.countDown()
    latch.await()
    while (count > 0) {
      count = count - 1

      val incDecCount = random.nextInt(10)
      val reqServeCount = random.nextInt(10)

      (1 to incDecCount).foreach { _ =>
        tracker.increment()
        tracker.decrement()
      }
      (1 to reqServeCount).foreach{_ =>
        tracker.request(1)
        tracker.addServed(1)
      }

      // Because this test 'requests' exactly how much data exists, there is a race between decrementing the tracker and decrementing demand.
      // For that reason, decrement must be called first to count down the latch, which will also update the status correctly.
      tracker.decrement()
      tracker.addServed(1)
    }
  }
}
