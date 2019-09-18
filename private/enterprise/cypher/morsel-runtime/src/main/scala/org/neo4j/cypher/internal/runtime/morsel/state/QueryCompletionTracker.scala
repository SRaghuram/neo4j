/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.FlowControl
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner.Thunk
import org.neo4j.internal.kernel.api.exceptions.LocksNotFrozenException
import org.neo4j.kernel.impl.query.QuerySubscriber

import scala.collection.mutable.ArrayBuffer

/**
  * A [[QueryCompletionTracker]] tracks the progress of a query. This is done by keeping an internal
  * count of events. When the count is zero the query has completed.
  */
trait QueryCompletionTracker extends FlowControl {
  /**
    * Increment the tracker count.
    */
  def increment(): Unit

  /**
    * Increment by n
    */
  def incrementBy(n: Long): Unit

  /**
    * Decrement the tracker count.
    */
  def decrement(): Unit

  /**
    * Decrement by n
    */
  def decrementBy(n: Long): Unit

  /**
    * Error!
    */
  def error(throwable: Throwable): Unit

  /**
    * Checks if the query has ended. Non-blocking. This method can return
    * true if all query work is done, if the query was cancelled, or if
    * an exception occurred.
    */
  def hasEnded: Boolean

  def peekError: Throwable

  /**
    * Add an assertion to be run when the query is completed.
    */
  def addCompletionAssertion(assertion: Thunk): Unit = {
    if (AssertionRunner.isAssertionsEnabled) {
      assertions += assertion
    }
  }

  protected val assertions: ArrayBuffer[Thunk] =
    if (AssertionRunner.isAssertionsEnabled)
      new ArrayBuffer[Thunk]()
    else null

  protected def runAssertions(): Unit = {
    if (AssertionRunner.isAssertionsEnabled) {
      assertions.foreach(_.apply())
    }
  }

  private val instanceName = if (DebugSupport.TRACKER.enabled) s"[${getClass.getSimpleName}@${System.identityHashCode(this)}] " else ""

  def debug(str: String): Unit =
    if (DebugSupport.TRACKER.enabled) {
      DebugSupport.TRACKER.log(instanceName + str)
    }

  def debug(str: String, x: Any): Unit =
    if (DebugSupport.TRACKER.enabled) {
      DebugSupport.TRACKER.log(instanceName + str, x)
    }

  def debug(str: String, x1: Any, x2: Any): Unit =
    if (DebugSupport.TRACKER.enabled) {
      DebugSupport.TRACKER.log(instanceName + str, x1, x2)
    }

  def debug(str: String, x1: Any, x2: Any, x3: Any): Unit =
    if (DebugSupport.TRACKER.enabled) {
      DebugSupport.TRACKER.log(instanceName + str, x1, x2, x3)
    }
}

/**
  * Not thread-safe implementation of [[QueryCompletionTracker]].
  */
class StandardQueryCompletionTracker(subscriber: QuerySubscriber,
                                     queryContext: QueryContext,
                                     tracer: QueryExecutionTracer) extends QueryCompletionTracker {
  private var count = 0L
  private var throwable: Throwable = _
  private var demand = 0L
  private var cancelled = false

  override def increment(): Unit = {
    count += 1
    debug("Incremented to %d", count)
  }

  override def incrementBy(n: Long): Unit = {
    if (n != 0) {
      count += n
      debug("Incremented by %d to %d", n, count)
    }
  }

  override def decrement(): Unit = {
    count -= 1
    debug("Decremented to %d", count)
    if (count < 0) {
      throw new IllegalStateException(s"Should not decrement below zero: $count")
    }
    postDecrement()
  }

  override def decrementBy(n: Long): Unit = {
    if (n != 0) {
      count -= n
      debug("Decremented by %d to %d", n, count)
      postDecrement()
    }
  }

  private def postDecrement(): Unit = {
    if (count == 0) {
      try {
        if (throwable != null) {

          subscriber.onError(throwable)
        } else if (!cancelled) {
          subscriber.onResultCompleted(queryContext.getOptStatistics.getOrElse(QueryStatistics()))
        }
      } finally {
        tracer.stopQuery()
      }
    }
  }

  override def error(throwable: Throwable): Unit = {
    this.throwable = throwable
  }

  override def getDemand: Long = demand

  override def hasDemand: Boolean = getDemand > 0

  override def addServed(newlyServed: Long): Unit = {
    demand -= newlyServed
  }

  override def hasEnded: Boolean = count == 0

  override def peekError: Throwable = throwable

  // -------- Subscription Methods --------

  override def request(numberOfRecords: Long): Unit = {
    val newDemand = demand + numberOfRecords
    //check for overflow, this might happen since Bolt sends us `Long.MAX_VALUE` for `PULL_ALL`
    demand = if (newDemand < 0) {
      Long.MaxValue
    } else {
      newDemand
    }
  }

  override def cancel(): Unit = {
    cancelled = true
  }

  override def await(): Boolean = {
    if (throwable != null) {
      throw throwable
    }

    if (count != 0 && !cancelled && demand > 0) {
      throw new IllegalStateException(
        s"""Should not reach await until tracking is complete, cancelled or out-of demand,
           |count: $count, cancelled: $cancelled, demand: $demand""".stripMargin)
    }

    val moreToCome = count > 0 && !cancelled
    if (!moreToCome) {
      runAssertions()
    }

    moreToCome
  }

  override def toString: String = s"StandardQueryCompletionTracker($count)"
}

/**
  * Concurrent implementation of [[QueryCompletionTracker]].
  */
class ConcurrentQueryCompletionTracker(subscriber: QuerySubscriber,
                                       queryContext: QueryContext,
                                       tracer: QueryExecutionTracer) extends QueryCompletionTracker {

  // Count of "things" that haven't been closed yet
  private val count = new AtomicLong(0)

  // Errors that happened during execution
  private val errors = new ConcurrentLinkedQueue[Throwable]()

  @volatile private var _cancelledOrFailed = false
  @volatile private var _hasEnded = false

  // Requested number of rows
  private val requested = new AtomicLong(0)

  // Served number of rows
  private val served = new AtomicLong(0)

  // List of latches that are waited on. Note that because requested is monotonically incremented, waiters will typically be in requestedRow ascending order.
  private val waiters = new ConcurrentLinkedQueue[WaitState]()
  case class WaitState(latch: CountDownLatch, requestedRows: Long)

  override def toString: String = s"[ConcurrentQueryCompletionTracker@${System.identityHashCode(this)}](${count.get()})"

  // -------- Query completion tracking methods --------

  override def increment(): Unit = {
    if (_hasEnded) {
      throw new IllegalStateException(s"Increment called even though query has ended. That should not happen. Current count: ${count.get()}")
    }
    val newCount = count.incrementAndGet()
    debug("Increment to %d", newCount)
  }

  override def incrementBy(n: Long): Unit = {
    if (n != 0) {
      val newCount = count.addAndGet(n)
      debug("Increment by %d to %d", n, newCount)
    }
  }

  override def decrement(): Unit = {
    val newCount = count.decrementAndGet()
    debug("Decrement to %d", newCount)
    postDecrement(newCount)
  }

  override def decrementBy(n: Long): Unit = {
    if (n != 0) {
      val newCount = count.addAndGet(-n)
      debug("Decrement by %d to %d", n, newCount)
      postDecrement(newCount)
    }
  }

  private def postDecrement(newCount: Long): Unit = {
    if (newCount == 0) {
      try {
        reportQueryEndToSubscriber()
        thawTransactionLocks()

        // IMPORTANT: update _hasEnded before releasing waiters, to coordinate properly with await().
        _hasEnded = true

        waiters.forEach(waitState => waitState.latch.countDown())
        waiters.clear()
      } finally {
        tracer.stopQuery()
      }
    } else if (newCount < 0) {
      throw new IllegalStateException("Cannot count below 0")
    }
  }

  private def thawTransactionLocks(): Unit = {
    try {
      queryContext.transactionalContext.transaction.thawLocks()
    } catch {
      case _: LocksNotFrozenException =>
        // locks are already thawed, nothing more to do
      case thawError: Throwable => // unexpected, stash and continue
        errors.add(thawError)
    }
  }

  private def reportQueryEndToSubscriber(): Unit = {
    try {
      if (!errors.isEmpty) {
        subscriber.onError(allErrors())
      } else if (_cancelledOrFailed) {
        // Nothing to do for now. Probably a subscriber.onCancelled callback later
      } else {
        subscriber.onResultCompleted(queryContext.getOptStatistics.getOrElse(QueryStatistics()))
      }
    } catch {
      case reportError: Throwable =>
        errors.add(reportError)
    }
  }

  override def error(throwable: Throwable): Unit = {
    errors.add(throwable) // add error first to avoid seeing this as a cancellation in postDecrement()
    _cancelledOrFailed = true
  }

  override def peekError: Throwable = errors.peek

  override def hasEnded: Boolean = _hasEnded

  // -------- Flow control methods --------

  // We avoid ProduceResults (which reads this) doing any more work if the query is cancelled or had an error
  override def getDemand: Long = if (_cancelledOrFailed) 0 else requested.get() - served.get()

  override def hasDemand: Boolean = getDemand > 0

  override def addServed(newlyServed: Long): Unit = {
    debug("Served %d rows", newlyServed)
    // IMPORTANT: update served before releasing waiters, to coordinate properly with await().
    val newServed = served.addAndGet(newlyServed)
    var waitState = waiters.peek()
    while (waitState != null && waitState.requestedRows <= newServed) {
      debug("Releasing latch %s", waitState.latch)
      waitState.latch.countDown()
      waiters.poll()
      waitState = waiters.peek()
    }
  }

  // -------- Subscription methods --------

  override def cancel(): Unit = {
    _cancelledOrFailed = true
    debug("Cancelled")
  }

  override def request(numberOfRecords: Long): Unit = {
    if (numberOfRecords > 0) {
      debug("Request %d rows", numberOfRecords)

      requested.accumulateAndGet(numberOfRecords, (oldVal, newVal) => {
        val newDemand = oldVal + newVal
        //check for overflow, this might happen since Bolt sends us `Long.MAX_VALUE` for `PULL_ALL`
        if (newDemand < 0) {
          Long.MaxValue
        } else {
          newDemand
        }
      })
    }
  }

  override def await(): Boolean = {
    val moreToCome =
      if (hasEnded) {
        debug("Awaiting query which has ended")
        false
      } else {
        val currentRequested = requested.get()
        val currentServed = served.get()
        if (currentRequested > currentServed) {
          val latch = new CountDownLatch(1)
          waiters.offer(WaitState(latch, currentRequested))

          // We re-read served and hasEnded here to guard against concurrent serving or query completion having happened
          // in between the first reads and adding the latch to waiters. If we didn't do this, we could leave this latch
          // at 1 forever.
          if (served.get() >= currentRequested || hasEnded) {
            latch.countDown()
          }
          debug("Awaiting latch %s ....", latch)
          latch.await()
          debug("Awaiting latch %s done", latch)
        }
        !hasEnded
      }

    if (!errors.isEmpty) {
      throw allErrors()
    }
    if (!moreToCome) {
      runAssertions()
    }
    moreToCome
  }

  private def allErrors(): Throwable = {
    val first = errors.peek()
    errors.forEach(t => if (t != first) first.addSuppressed(t))
    first
  }
}
