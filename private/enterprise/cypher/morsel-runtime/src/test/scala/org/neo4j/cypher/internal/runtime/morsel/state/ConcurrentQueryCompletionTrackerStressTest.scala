/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent._

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.QuerySubscriber

class ConcurrentQueryCompletionTrackerStressTest extends CypherFunSuite {

  private val THREAD_PAIRS = 6

  test("should handle concurrent QueryRunner and Worker") {

    // When
    val stopSignal = new AtomicBoolean(false)
    val errors = new ConcurrentLinkedQueue[Throwable]()

    val threads =
      for {
        i <- 0 until THREAD_PAIRS
        ongoingWork = new ArrayBlockingQueue[QueryCompletionTracker](8)
        runnable <- List(new QueryRunner(ongoingWork, stopSignal),
                         new Worker(ongoingWork, stopSignal))
      } yield {
        val thread = new Thread(runnable)
        thread.setName(runnable.getClass.getSimpleName)
        thread.start()
        thread.setUncaughtExceptionHandler((_, e) => errors.add(e))
        thread
      }

    // Then
    Thread.sleep(2000)
    stopSignal.set(true)

    for (thread <- threads) {
      thread.join(1000)
      if (thread.isAlive) {
        thread.interrupt()
        thread.join(1000)
        if (thread.isAlive) {
          fail(s"Thread $thread hung indefinitely and was not interruptable")
        }
      }
    }

    if (!errors.isEmpty) {
      val exception = errors.poll()
      var anotherException = errors.poll()
      while (anotherException != null) {
        exception.addSuppressed(anotherException)
        anotherException = errors.poll()
      }
      throw exception
    }
  }

  class QueryRunner(ongoingWork: ArrayBlockingQueue[QueryCompletionTracker],
                    stopSignal: AtomicBoolean) extends Runnable {

    private val random = ThreadLocalRandom.current()
    private var totalQueriesStarted = 0L
    private var totalRequestsServed = 0L

    override def run(): Unit = {
      try {
        while (!stopSignal.get()) {
          val newQuery = new ConcurrentQueryCompletionTracker(mock[QuerySubscriber],
            mock[QueryContext](RETURNS_DEEP_STUBS),
            mock[QueryExecutionTracer])

          newQuery.increment()
          ongoingWork.put(newQuery)
          totalQueriesStarted += 1

          var request = random.nextInt(3) + 1
          while (request > 0) {
            newQuery.request(request)

            val moreToCome = newQuery.await()
            totalRequestsServed += request
            if (moreToCome) {
              request = random.nextInt(10) + 1
            } else {
              request = 0
            }
          }
          val moreToCome = newQuery.await()
          moreToCome shouldBe false
          newQuery.hasEnded shouldBe true
        }
      } catch {
        case e: InterruptedException =>
          fail(s"QueryRunner hung after starting $totalQueriesStarted queries and consuming $totalRequestsServed rows", e)
      }
    }
  }

  class Worker(ongoingWork: ArrayBlockingQueue[QueryCompletionTracker],
               stopSignal: AtomicBoolean) extends Runnable {

    private val random = ThreadLocalRandom.current()

    override def run(): Unit = {
      while (!stopSignal.get()) {
        val query = ongoingWork.take()
        emulateQuery(query)
      }

      val finalQuery = ongoingWork.poll()
      if (finalQuery != null) {
        emulateQuery(finalQuery)
      }
    }

    private def emulateQuery(query: QueryCompletionTracker): Unit = {
      val queryWorkItems = random.nextInt(100)

      var i = queryWorkItems
      while (i > 0) {
        query.increment()
        if (random.nextBoolean() && query.hasDemand) {
          query.addServed(1)
        }
        query.decrement()
        i -= 1
      }
      query.decrement() // final decrement
    }
  }
}
