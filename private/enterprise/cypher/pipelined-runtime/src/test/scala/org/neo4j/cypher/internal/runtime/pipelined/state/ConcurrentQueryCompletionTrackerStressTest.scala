/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ConcurrentQueryCompletionTrackerStressTest.POISON_PILL
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.kernel.impl.query.QuerySubscriber

class ConcurrentQueryCompletionTrackerStressTest extends CypherFunSuite with TimeLimitedCypherTest {

  private val THREAD_PAIRS = 6

  test("should handle concurrent QueryRunner and Worker") {

    // When
    val stopSignal = new AtomicBoolean(false)
    val errors = new ConcurrentLinkedQueue[Throwable]()

    val threads =
      for {
        _ <- 0 until THREAD_PAIRS
        ongoingWork = new ArrayBlockingQueue[QueryCompletionTracker](8)
        runnable <- List(new QueryRunner(ongoingWork, stopSignal),
          new Worker(ongoingWork))
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
      thread.join(10000)
      if (thread.isAlive) {
        thread.interrupt()
        thread.join(10000)
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
    private val querySubscriber = mock[QuerySubscriber]
    private val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    private val executionTracer = mock[QueryExecutionTracer]

    private val random = ThreadLocalRandom.current()
    private var totalQueriesStarted = 0L
    private var totalRequestsServed = 0L

    override def run(): Unit = {
      try {
        while (!stopSignal.get()) {

          val newQuery = new ConcurrentQueryCompletionTracker(querySubscriber,
            context,
            executionTracer,
            mock[QueryResources](RETURNS_DEEP_STUBS))

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
      } finally {
        ongoingWork.put(POISON_PILL)
      }
    }
  }

  class Worker(ongoingWork: ArrayBlockingQueue[QueryCompletionTracker]) extends Runnable {

    private val random = ThreadLocalRandom.current()

    override def run(): Unit = {
      var break = false
      while (!break) {
        val query = ongoingWork.take()
        break = query eq POISON_PILL
        if (!break) {
          emulateQuery(query)
        }
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

object ConcurrentQueryCompletionTrackerStressTest {
  private val POISON_PILL: QueryCompletionTracker = new QueryCompletionTracker {
    override def increment(): Unit = fail()
    override def incrementBy(n: Long): Unit = fail()
    override def decrement(): Unit = fail()
    override def decrementBy(n: Long): Unit = fail()
    override def error(throwable: Throwable): Unit = fail()
    override def hasEnded: Boolean = fail()
    override def hasSucceeded: Boolean = fail()
    override def getDemandUnlessCancelled: Long = fail()
    override def hasDemand: Boolean = fail()
    override def addServed(newlyServed: Long): Unit = fail()
    override def request(numberOfRecords: Long): Unit = fail()
    override def cancel(): Unit = fail()
    override def await(): Boolean = fail()

    private def fail() =
      throw new AssertionError("This is not the QueryCompletionTracker you are looking for")
  }
}
