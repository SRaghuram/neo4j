/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}

import org.neo4j.cypher.internal.runtime.morsel.FlowControl
import org.neo4j.cypher.internal.runtime.zombie.Zombie
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * A [[QueryCompletionTracker]] tracks the progress of a query. This is done by keeping an internal
  * count of events. When the count is zero the query has completed.
  */
trait QueryCompletionTracker extends FlowControl {
  /**
    * Increment the tracker count.
    */
  def increment(): Long

  /**
    * Decrement the tracker count.
    */
  def decrement(): Long

  /**
    * Error!
    */
  def error(throwable: Throwable): Unit

  /**
    * Query completion state. Non-blocking.
    *
    * @return true iff the query has completed
    */
  def isCompleted: Boolean
}

/**
  * Not thread-safe implementation of [[QueryCompletionTracker]].
  */
class StandardQueryCompletionTracker(subscriber: QuerySubscriber,
                                     queryContext: QueryContext) extends QueryCompletionTracker {
  private var count = 0L
  private var throwable: Throwable = _
  private var demand = 0L
  private var cancelled = false

  override def increment(): Long = {
    count += 1
    count
  }

  override def decrement(): Long = {
    count -= 1
    if (count < 0) {
      throw new IllegalStateException(s"Should not decrement below zero: $count")
    }
    if (count == 0) {
      subscriber.onResultCompleted(queryContext.getOptStatistics.getOrElse(QueryStatistics()))
    }
    count
  }

  override def error(throwable: Throwable): Unit = {
    this.throwable = throwable
    subscriber.onError(throwable)
  }

  override def getDemand: Long = demand

  override def hasDemand: Boolean = getDemand > 0

  override def addServed(newlyServed: Long): Unit = {
    demand -= newlyServed
  }

  override def isCompleted: Boolean = throwable != null || count == 0 || cancelled

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

    count > 0 && !cancelled
  }

  override def toString: String = s"StandardQueryCompletionTracker($count)"
}

/**
  * Concurrent implementation of [[QueryCompletionTracker]].
  */
class ConcurrentQueryCompletionTracker(subscriber: QuerySubscriber,
                                       queryContext: QueryContext) extends QueryCompletionTracker {
  private val count = new AtomicLong(0)
  private val errors = new ConcurrentLinkedQueue[Throwable]()
  private var latch = new CountDownLatch(1)
  private val demand = new AtomicLong(0)
  private val cancelled = new AtomicBoolean(false)

  override def increment(): Long = {
    val newCount = count.incrementAndGet()
    Zombie.debug(s"Incremented ${getClass.getSimpleName}. New count: $newCount")
    newCount
  }

  override def decrement(): Long = {
    val newCount = count.decrementAndGet()
    Zombie.debug(s"Decremented ${getClass.getSimpleName}. New count: $newCount")
    if (newCount == 0) {
      synchronized {
        try {
          subscriber.onResultCompleted(queryContext.getOptStatistics.getOrElse(QueryStatistics()))
        } finally {
          releaseLatch()
        }
      }
    }
    else if (newCount < 0)
      throw new IllegalStateException("Cannot count below 0")
    newCount
  }

  override def error(throwable: Throwable): Unit = {
    try {
      errors.add(throwable)
      synchronized {
        subscriber.onError(throwable)
      }
    }
    finally {
      releaseLatch()
    }
  }

  override def isCompleted: Boolean = count.get == 0 || cancelled.get()

  override def toString: String = s"ConcurrentQueryCompletionTracker(${count.get()})"

  override def getDemand: Long = demand.get()

  override def hasDemand: Boolean = getDemand > 0

  override def addServed(newlyServed: Long): Unit = {
    val newDemand = demand.addAndGet(-newlyServed)
    if (newDemand == 0) {
      releaseLatch()
    }
  }

  // -------- Subscription Methods --------

  override def request(numberOfRecords: Long): Unit = {
    if (!isCompleted) {
      //there is new demand make sure to reset the latch
      if (numberOfRecords > 0) {
        synchronized {
          if (latch.getCount == 0) {
            latch = new CountDownLatch(1)
          }
        }
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

  override def cancel(): Unit = {
    try {
      cancelled.set(true)
    } finally {
      releaseLatch()
    }
  }

  override def await(): Boolean = {
    latch.await()
    if (!errors.isEmpty) {
      val firstException = errors.poll()
      errors.forEach(t => firstException.addSuppressed(t))
      throw firstException
    }
    !isCompleted
  }

  private def releaseLatch(): Unit = {
    synchronized {
      latch.countDown()
    }
  }
}
