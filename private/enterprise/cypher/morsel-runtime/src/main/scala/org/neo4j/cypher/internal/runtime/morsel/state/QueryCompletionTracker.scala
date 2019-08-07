/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}

import org.neo4j.cypher.exceptionHandler
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.execution.FlowControl
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner.Thunk
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
    * Query completion state. Non-blocking.
    *
    * @return true iff the query has completed. A query is completed when it delivered all data.
    */
  def isCompleted: Boolean

  /**
    * Add an assertion to be run when the query is completed.
    */
  def addCompletionAssertion(assertion: Thunk): Unit = {
    if (AssertionRunner.isAssertionsEnabled) {
      assertions += assertion
    }
  }

  protected val assertions: ArrayBuffer[Thunk] = new ArrayBuffer[Thunk]()

  protected def runAssertions(): Unit = {
    assertions.foreach(AssertionRunner.runUnderAssertion)
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
    DebugSupport.logTracker(s"Incremented ${getClass.getSimpleName}. New count: $count")
  }

  override def incrementBy(n: Long): Unit = {
    if (n != 0) {
      count += n
      DebugSupport.logTracker(s"Incremented ${getClass.getSimpleName} by $n. New count: $count")
    }
  }

  override def decrement(): Unit = {
    count -= 1
    DebugSupport.logTracker(s"Decremented ${getClass.getSimpleName}. New count: $count")
    if (count < 0) {
      throw new IllegalStateException(s"Should not decrement below zero: $count")
    }
    postDecrement()
  }

  override def decrementBy(n: Long): Unit = {
    if (n != 0) {
      count -= n
      DebugSupport.logTracker(s"Decremented ${getClass.getSimpleName} by $n. New count: $count")
      postDecrement()
    }
  }

  private def postDecrement(): Unit = {
    if (count == 0) {
      try {
        if (throwable != null) {

          subscriber.onError(exceptionHandler.mapToCypher(throwable))
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

  override def isCompleted: Boolean = count == 0

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

  // Used to implement await. Released each time we meet the demand or the query is done
  private var latch = new CountDownLatch(1)

  // Current demand in number of rows
  private val demand = new AtomicLong(0)

  sealed trait Status
  case object Running extends Status
  case object CountReachedZero extends Status
  case object Errors extends Status
  case object Cancelled extends Status

  // The status of the query
  private val status = new AtomicReference[Status](Running)

  override def increment(): Unit = {
    AssertionRunner.runUnderAssertion { () =>
      status.get() match {
        case CountReachedZero =>
          throw new IllegalStateException(s"Increment called even though CountReachedZero. That should not happen. Current count: ${count.get()}")
        case _ => // Nothing to do
      }
    }
    val newCount = count.incrementAndGet()
    DebugSupport.logTracker(s"Incremented $toString. New count: $newCount")
  }

  override def incrementBy(n: Long): Unit = {
    if (n != 0) {
      val newCount = count.addAndGet(n)
      DebugSupport.logTracker(s"Incremented ${getClass.getSimpleName} by $n. New count: $newCount")
    }
  }

  override def decrement(): Unit = {
    val newCount = count.decrementAndGet()
    DebugSupport.logTracker(s"Decremented $toString. New count: $newCount")
    postDecrement(newCount)
  }

  override def decrementBy(n: Long): Unit = {
    if (n != 0) {
      val newCount = count.addAndGet(-n)
      DebugSupport.logTracker(s"Decremented ${getClass.getSimpleName} by $n. New count: $newCount")
      postDecrement(newCount)
    }
  }

  private def postDecrement(newCount: Long): Unit = {
    if (newCount == 0) {
      try {
        status.compareAndExchange(Running, CountReachedZero) match {
          case CountReachedZero =>
            throw new IllegalStateException("Someone else updated the state to CountReachedZero. That should not happen.")
          case Errors =>
            subscriber.onError(exceptionHandler.mapToCypher(allErrors()))
          case Running =>
            subscriber.onResultCompleted(queryContext.getOptStatistics.getOrElse(QueryStatistics()))
          case Cancelled =>
            // Nothing to do for now. Probably a subscriber.onCancelled callback later
        }
      } finally {
        tracer.stopQuery()
        queryContext.transactionalContext.transaction.thawLocks()
        releaseLatch()
      }
    } else if (newCount < 0) {
      throw new IllegalStateException("Cannot count below 0")
    }
  }

  override def error(throwable: Throwable): Unit = {
    // First add and then set the status, to avoid the situation where decrement encounters the status `Error` but has no error to report.
    errors.add(throwable)
    status.set(Errors)
  }

  override def isCompleted: Boolean = count.get() == 0

  override def toString: String = s"ConcurrentQueryCompletionTracker ${System.identityHashCode(this)}(${count.get()})"

  // We avoid ProduceResults (which reads this) doing any more work if the query is cancelled or had an error
  override def getDemand: Long = if (status.get() != Running) 0 else demand.get()

  override def hasDemand: Boolean = getDemand > 0

  override def addServed(newlyServed: Long): Unit = {
    DebugSupport.logTracker(s"Subtracting $newlyServed of demand in $toString")
    val newDemand = demand.addAndGet(-newlyServed)
    if (newDemand == 0) {
      releaseLatch()
    }
  }

  // -------- Subscription Methods --------

  override def cancel(): Unit = {
    if (status.compareAndSet(Running, Cancelled)) {
      DebugSupport.logTracker(s"Canceled $toString")
    }
  }

  override def request(numberOfRecords: Long): Unit = {
    // Instead of just adding demand when the state is `Running`, we also do it in the case of error or cancel.
    // Otherwise we would count down the latch immediately on any subsequent `await` call, and not allow
    // for proper cleanup of in-flight tasks.
    if (status.get() != CountReachedZero) {
      //there is new demand make sure to reset the latch
      if (numberOfRecords > 0) {
        resetLatch()
        DebugSupport.logTracker(s"Adding $numberOfRecords to demand in $toString")
        demand.accumulateAndGet(numberOfRecords, (oldVal, newVal) => {
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
  }

  override def await(): Boolean = {
    DebugSupport.logTracker(s"Awaiting latch $latch in $toString ....")
    latch.await()
    DebugSupport.logTracker(s"Awaiting latch $latch in $toString done")
    if (!errors.isEmpty) {
      throw allErrors()
    }
    val moreToCome = status.get() == Running
    if (!moreToCome) {
      runAssertions()
    }
    moreToCome
  }

  private def releaseLatch(): Unit = this.synchronized {
    DebugSupport.logTracker(s"Releasing latch $latch in $toString")
    latch.countDown()
  }

  private def resetLatch(): Unit = this.synchronized {
    if (latch.getCount == 0) {
      val oldLatch = latch
      latch = new CountDownLatch(1)
      DebugSupport.logTracker(s"Resetting latch in $toString. Old: $oldLatch. new: $latch")
    }
  }

  private def allErrors(): Throwable = {
    val first = errors.peek()
    errors.forEach(t => if (t != first) first.addSuppressed(t))
    first
  }
}
