/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.FlowControl
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.internal.kernel.api.exceptions.LocksNotFrozenException
import org.neo4j.kernel.impl.query.QuerySubscriber

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class QueryTrackerKey(key: String) extends AnyVal

/**
 * A [[QueryCompletionTracker]] tracks the progress of a query. This is done by keeping an internal
 * count of events. When the count is zero the query has completed.
 */
trait QueryCompletionTracker extends FlowControl {
  /**
   * Increment the tracker count.
   *
   * @param key tracking key, implementations are not required to use this
   */
  def increment(key: QueryTrackerKey): Unit

  /**
   * Increment by n
   *
   * @param key tracking key, implementations are not required to use this
   * @param n increment
   */
  def incrementBy(key: QueryTrackerKey, n: Long): Unit

  /**
   * Decrement the tracker count.
   *
   * @param key tracking key, implementations are not required to use this
   */
  def decrement(key: QueryTrackerKey): Unit

  /**
   * Decrement by n
   *
   * @param key tracking key, implementations are not required to use this
   * @param n decrement
   */
  def decrementBy(key: QueryTrackerKey, n: Long): Unit

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

  /**
   * Checks if the query has ended. Non-blocking. This method can return
   * true if all query work is done.
   *
   * If the query was cancelled, or if an exception occurred, return false.
   */
  def hasSucceeded: Boolean

  /**
   * Add an assertion to be run when the query is completed.
   */
  def addCompletionAssertion(assertion: () => Unit): Boolean = {
    getOrCreateAssertions() += assertion
    true
  }

  private[this] var _assertions: ArrayBuffer[() => Unit] = _

  protected def runAssertions(): Boolean = {
    getOrCreateAssertions().foreach(_.apply())
    true
  }

  private def getOrCreateAssertions() = {
    if (_assertions == null) {
      _assertions = new ArrayBuffer[() => Unit]()
    }
    _assertions
  }

  private val instanceName = if (DebugSupport.DEBUG_TRACKER) s"[${getClass.getSimpleName}@${System.identityHashCode(this)}] " else ""

  def debug(str: String): Unit =
    if (DebugSupport.DEBUG_TRACKER) {
      DebugSupport.TRACKER.log(instanceName + str)
    }

  def debug(str: String, x: Any): Unit =
    if (DebugSupport.DEBUG_TRACKER) {
      DebugSupport.TRACKER.log(instanceName + str, x)
    }

  def debug(str: String, x1: Any, x2: Any): Unit =
    if (DebugSupport.DEBUG_TRACKER) {
      DebugSupport.TRACKER.log(instanceName + str, x1, x2)
    }

  def debug(str: String, x1: Any, x2: Any, x3: Any): Unit =
    if (DebugSupport.DEBUG_TRACKER) {
      DebugSupport.TRACKER.log(instanceName + str, x1, x2, x3)
    }
}

abstract class AbstractStandardQueryCompletionTracker(
  private[this] val subscriber: QuerySubscriber,
  private[this] val resources: QueryResources,
  private[this] val tracer: QueryExecutionTracer
) extends QueryCompletionTracker {
  private[this] var _throwable: Throwable = _
  private[this] var _demand = 0L
  private[this] var _cancelled = false
  private[this] var _hasEnded = false
  private[this] var _hasSucceeded = false

  override def error(throwable: Throwable): Unit = {
    if (this._throwable == null) {
      this._throwable = throwable
    } else {
      if (this._throwable != throwable) {
        this._throwable.addSuppressed(throwable)
      }
    }
  }

  override def getDemandUnlessCancelled: Long = _demand

  override def hasDemand: Boolean = _demand > 0

  override def addServed(newlyServed: Long): Unit = {
    _demand -= newlyServed
  }

  override def hasEnded: Boolean = _hasEnded

  override def hasSucceeded: Boolean = _hasSucceeded

  // -------- Subscription Methods --------

  override def request(numberOfRecords: Long): Unit = {
    val newDemand = _demand + numberOfRecords
    //check for overflow, this might happen since Bolt sends us `Long.MAX_VALUE` for `PULL_ALL`
    _demand = if (newDemand < 0) {
      Long.MaxValue
    } else {
      newDemand
    }
  }

  override def cancel(): Unit = {
    _cancelled = true
  }

  override def await(): Boolean = {
    if (_throwable != null) {
      throw _throwable
    }

    if (count != 0 && !_cancelled && _demand > 0) {
      // TODO Different message in debug implementation
      throw new IllegalStateException(
        s"""Should not reach await until tracking is complete, cancelled or out-of demand,
           |count: ${count}, cancelled: ${_cancelled}, demand: ${_demand}""".stripMargin)
    }

    val moreToCome = count > 0 && !_cancelled
    if (!moreToCome) {
      runAssertions()
    }

    moreToCome
  }

  private def postDecrement(key: QueryTrackerKey): Unit = {
    verifyCount(key)
    if (isCountComplete && !_hasEnded) {
      try {
        if (throwable != null) {
          subscriber.onError(throwable)
        } else if (!isCancelled) {
          subscriber.onResultCompleted(resources.queryStatisticsTracker)
        }
      } catch {
        case NonFatalCypherError(reportError) =>
          error(reportError) // stash and continue
      }
      tracer.stopQuery()
      _hasEnded = true
      if (!isCancelled && throwable == null) {
        _hasSucceeded = true
      }
    }
  }

  override final def decrement(key: QueryTrackerKey): Unit = {
    decrementCount(key)
    postDecrement(key)
  }

  override final def decrementBy(key: QueryTrackerKey, n: Long): Unit = {
    decrementCountBy(key, n)
    postDecrement(key)
  }

  protected def throwable: Throwable = _throwable
  protected def isCancelled: Boolean = _cancelled

  /**
   * Decrement count by 1.
   *
   * @param key tracking key, implementations are not required to use this
   */
  protected def decrementCount(key: QueryTrackerKey): Unit

  /**
   * Decrement count by n.
   *
   * @param key tracking key, implementations are not required to use this
   * @param n decrement amount
   */
  protected def decrementCountBy(key: QueryTrackerKey, n: Long): Unit

  /**
   * Returns the total count if this tracker.
   */
  protected def count: Long

  /**
   * Verify the count of this tracker. Incorrect counts should be reported with [[QueryCompletionTracker.error()]].
   *
   * @param key tracking key, implementations are not required to use this
   */
  protected def verifyCount(key: QueryTrackerKey): Unit

  /**
   * Returns true if count is complete.
   */
  protected def isCountComplete: Boolean
}

/**
 * Not thread-safe implementation of [[QueryCompletionTracker]].
 */
class StandardQueryCompletionTracker(
  subscriber: QuerySubscriber,
  tracer: QueryExecutionTracer,
  resources: QueryResources
) extends AbstractStandardQueryCompletionTracker(subscriber, resources, tracer) {
  private[this] var _count = 0L

  override def increment(key: QueryTrackerKey): Unit = {
    _count += 1
  }

  override def incrementBy(key: QueryTrackerKey, n: Long): Unit = {
    if (n != 0) {
      _count += n
    }
  }

  override def decrementCount(key: QueryTrackerKey): Unit = {
    _count -= 1
  }

  override def decrementCountBy(key: QueryTrackerKey, n: Long): Unit = {
    if (n != 0) {
      _count -= n
    }
  }

  override def toString: String = s"StandardQueryCompletionTracker(${_count})"

  override protected def count: Long = _count

  override protected def verifyCount(key: QueryTrackerKey): Unit = {
    if (_count < 0) {
      error(new ReferenceCountingException("Cannot count below 0, but got count " + _count))
    }
  }

  override def isCountComplete: Boolean = {
    _count <= 0
  }
}

/**
 * Not thread-safe QueryCompletionTracker that provides extra debug support.
 *
 * - Keeps track of counts per reference
 * - Prints debug information
 */
class StandardDebugQueryCompletionTracker(
  subscriber: QuerySubscriber,
  tracer: QueryExecutionTracer,
  resources: QueryResources
) extends AbstractStandardQueryCompletionTracker(subscriber, resources, tracer) {

  private[this] val counts = mutable.Map.empty[QueryTrackerKey, Long]

  override def increment(key: QueryTrackerKey): Unit = {
    incrementBy(key, 1)
  }

  override def incrementBy(key: QueryTrackerKey, n: Long): Unit = {
    val newCount = counts.get(key) match {
      case Some(currentCount) => currentCount + n
      case _ => n
    }
    counts.update(key, newCount)
    debug("Incremented %s by %d to %d", key, n, newCount)
  }

  override def decrementCount(key: QueryTrackerKey): Unit = {
    decrementBy(key, 1)
  }

  override def decrementCountBy(key: QueryTrackerKey, n: Long): Unit = {
    val newCount = counts.get(key) match {
      case Some(currentCount) => currentCount - n
      case _ => -n
    }
    counts.update(key, newCount)
    debug("Decremented %s by %d to %d", key, n, newCount)
  }

  override protected def count: Long = counts.values.sum

  override protected def verifyCount(key: QueryTrackerKey): Unit = {
    val keyCount = counts.getOrElse(key, 0L)
    if (keyCount < 0) {
      // Note, this is more strict than the production implementation
      debugPrintCounts()
      error(new ReferenceCountingException(s"Cannot count below 0, but got count $keyCount for key '$key'"))
    }
  }

  override def isCountComplete: Boolean = {
    counts.valuesIterator.forall(count => count <= 0)
  }

  private def debugPrintCounts(): Unit = {
    counts.foreach { case (k, v) => debug(s"key: $k, count: $v") }
  }
}

/**
 * Base class that provides some implementation of a thread safe [[QueryCompletionTracker]].
 */
abstract class AbstractConcurrentQueryCompletionTracker(
  private[this] val subscriber: QuerySubscriber,
  private[this] val queryContext: QueryContext,
  private[this] val tracer: QueryExecutionTracer,
  private[this] val resources: QueryResources
) extends QueryCompletionTracker {
  // Errors that happened during execution
  private[this] val errors = new ConcurrentLinkedQueue[Throwable]()

  @volatile private[this] var _cancelledOrFailed = false
  @volatile private[this] var _hasEnded = false
  @volatile private[this] var _hasSucceeded = false

  // Requested number of rows
  private[this] val requested = new AtomicLong(0)

  // Served number of rows
  private[this] val served = new AtomicLong(0)

  // List of latches that are waited on. Note that because requested is monotonically incremented, waiters will typically be in requestedRow ascending order.
  private[this] val waiters = new ConcurrentLinkedQueue[WaitState]()
  case class WaitState(latch: CountDownLatch, requestedRows: Long)

  override def toString: String =
    s"[${getClass.getSimpleName}@${System.identityHashCode(this)}](" +
      s"count=${count}, " +
      s"requested=${requested.get()}, " +
      s"served=${served.get()}), " +
      s"cancelled/failed=${_cancelledOrFailed}), " +
      s"has ended=${_hasEnded})"

  // -------- Query completion tracking methods --------

  override final def decrement(key: QueryTrackerKey): Unit = {
    val newCount = decrementCount(key)
    postDecrement(newCount)
  }

  override final def decrementBy(key: QueryTrackerKey, n: Long): Unit = {
    if (n != 0) {
      val newCount = decrementCountBy(key, n)
      postDecrement(newCount)
    }
  }

  private def postDecrement(newTotalCount: Long): Unit = {
    if (newTotalCount <= 0) {
      if (newTotalCount < 0) {
        error(new ReferenceCountingException("Cannot count below 0, but got count " + newTotalCount))
      }
      if (!_hasEnded) {
        reportQueryEndToSubscriber()
        thawTransactionLocks()

        // IMPORTANT: update _hasEnded before releasing waiters, to coordinate properly with await().
        _hasEnded = true
        if (!_cancelledOrFailed) {
          _hasSucceeded = true
        }

        waiters.forEach(waitState => waitState.latch.countDown())
        waiters.clear()
        tracer.stopQuery()
      }
    }
  }

  private def thawTransactionLocks(): Unit = {
    try {
      queryContext.transactionalContext.transaction.thawLocks()
    } catch {
      case _: LocksNotFrozenException =>
      // locks are already thawed, nothing more to do
      case thawException: Exception => // unexpected, stash and continue
        error(thawException)
    }
  }

  private def reportQueryEndToSubscriber(): Unit = {
    try {
      if (!errors.isEmpty) {
        subscriber.onError(allErrors())
      } else if (_cancelledOrFailed) {
        // Nothing to do for now. Probably a subscriber.onCancelled callback later
      } else {
        subscriber.onResultCompleted(resources.queryStatisticsTracker)
      }
    } catch {
      case reportException: Exception =>
        error(reportException)
    }
  }

  override def error(throwable: Throwable): Unit = {
    errors.add(throwable) // add error first to avoid seeing this as a cancellation in postDecrement()
    _cancelledOrFailed = true
  }

  override def hasEnded: Boolean = _hasEnded

  override def hasSucceeded: Boolean = _hasSucceeded

  // -------- Flow control methods --------

  override def getDemandUnlessCancelled: Long = if (_cancelledOrFailed) 0 else requested.get() - served.get()

  // DANGER: Do not zero demand on cancellation, because this might lead to a rare racing hang on exceptions in
  //         the produce results pipeline in runtime=parallel, as long as we are practising soft closing of
  //         queries. If the race happens the query will never finish, so please be very careful around here.
  override def hasDemand: Boolean = (requested.get() - served.get()) > 0

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
          try {
            latch.await()
          } catch {
            case e: InterruptedException =>
              val e1 = new InterruptedException(toString)
              e1.addSuppressed(e)
              throw e1
          }
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

  protected def allErrors(): Throwable = {
    val first = errors.peek()
    errors.forEach(t => if (t != first) first.addSuppressed(t))
    first
  }

  /**
   * Returns total count
   */
  protected def count(): Long

  /**
   * Decrements count and returns the new count. Note, the returned count can be key count or total count depending on implementation.
   */
  protected def decrementCount(key: QueryTrackerKey): Long

  /**
   * Decrements count and returns the new count. Note, the returned count can be key count or total count depending on implementation.
   */
  protected def decrementCountBy(key: QueryTrackerKey, n: Long): Long

}

/**
 * Concurrent implementation of [[QueryCompletionTracker]].
 */
class ConcurrentQueryCompletionTracker(
  subscriber: QuerySubscriber,
  queryContext: QueryContext,
  tracer: QueryExecutionTracer,
  resources: QueryResources
) extends AbstractConcurrentQueryCompletionTracker(subscriber, queryContext, tracer, resources) {

  // Count of "things" that haven't been closed yet
  private val _count = new AtomicLong(0)

  override def increment(key: QueryTrackerKey): Unit = {
    if (hasEnded) {
      throw new ReferenceCountingException(s"Increment called even though query has ended. That should not happen. Current count: ${count}", allErrors())
    }
    _count.incrementAndGet()
  }

  override def incrementBy(key: QueryTrackerKey, n: Long): Unit = {
    if (hasEnded) {
      throw new ReferenceCountingException(s"Increment called even though query has ended. That should not happen. Current count: ${count}", allErrors())
    }
    if (n != 0) {
      _count.addAndGet(n)
    }
  }

  override def decrementCount(key: QueryTrackerKey): Long = {
    _count.decrementAndGet()
  }

  override def decrementCountBy(key: QueryTrackerKey, n: Long): Long = {
    _count.addAndGet(-n)
  }

  override protected def count(): Long = {
    _count.get()
  }

  override def toString: String = super.toString
}

/**
 * Thread-safe QueryCompletionTracker that provides extra debug support.
 *
 * - Keeps track of counts per reference
 * - Stricter count verification, all keys must complete
 * - Prints extra debug information
 */
class ConcurrentDebugQueryCompletionTracker(
  subscriber: QuerySubscriber,
  queryContext: QueryContext,
  tracer: QueryExecutionTracer,
  resources: QueryResources
) extends ConcurrentQueryCompletionTracker(subscriber, queryContext, tracer, resources) {

  // Count of "things" that haven't been closed yet
  private val counts = new ConcurrentHashMap[QueryTrackerKey, Long]()

  override def increment(key: QueryTrackerKey): Unit = {
    incrementBy(key, 1)
  }

  override def incrementBy(key: QueryTrackerKey, n: Long): Unit = {
    super.incrementBy(key, n)
    if (n != 0) {
      val newCount = counts.compute(key, (_, currentValue) => {
        if (currentValue != null.asInstanceOf[Long]) {
          currentValue + n
        } else {
          n
        }
      })
      debug("Incremented %s by %d to %d", key, n, newCount)
    }
  }

  override def decrementCount(key: QueryTrackerKey): Long = {
    decrementCountBy(key, 1)
  }

  override def decrementCountBy(key: QueryTrackerKey, n: Long): Long = {
    val newTotalCount = super.decrementCountBy(key, n)
    val newKeyCount = counts.compute(key, (_, currentValue) => {
      if (currentValue != null.asInstanceOf[Long]) {
        currentValue - n
      } else {
        -n
      }
    })
    debug("Decremented %s by %d to %d", key, n, newKeyCount)
    verifyCount(key, newKeyCount)
    newTotalCount
  }

  private def verifyCount(key: QueryTrackerKey, newCount: Long): Unit = {
    // Note, this is more strict than the production implementation
    if (newCount < 0) {
      val keyValueString = counts.entrySet().asScala.map( entry => s"key: ${entry.getKey}, count: ${entry.getValue}" ).mkString("\n")
      error(new ReferenceCountingException(s"Cannot count below 0, but got count $newCount for key '$key'. Counts: \n$keyValueString"))
    }
  }

  override def toString: String = super.toString
}
