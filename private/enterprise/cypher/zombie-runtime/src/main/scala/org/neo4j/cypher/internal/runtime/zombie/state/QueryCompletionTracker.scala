/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.internal.runtime.morsel.DemandControlSubscription
import org.neo4j.cypher.internal.runtime.zombie.Zombie
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * A [[QueryCompletionTracker]] tracks the progress of a query. This is done by keeping an internal
  * count of events. When the count is zero the query has completed.
  */
trait QueryCompletionTracker {
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
    * Await query completion.
    *
    * @retun when the query has completed successfully
    * @throws Throwable if an exception has occurred
    */
  def await(): Unit

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
                                     demandControlSubscription: DemandControlSubscription,
                                     queryContext: QueryContext) extends QueryCompletionTracker {
  private var count = 0L
  private var throwable: Throwable = _

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
      val statistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())
      subscriber.onResultCompleted(statistics)
      demandControlSubscription.setCompleted()
    }
    count
  }

  override def error(throwable: Throwable): Unit = {
    this.throwable = throwable
    subscriber.onError(throwable)
  }

  override def await(): Unit = {
    if (throwable != null) {
      throw throwable
    }
    if (count != 0 && !demandControlSubscription.isCompleteOrCancelled) {
      throw new IllegalStateException(s"Should not reach await until tracking is complete! count: $count")
    }
  }

  override def isCompleted: Boolean = throwable != null || count == 0 || demandControlSubscription.isCompleteOrCancelled

  override def toString: String = s"StandardQueryCompletionTracker($count)"
}

/**
  * Concurrent implementation of [[QueryCompletionTracker]].
  */
class ConcurrentQueryCompletionTracker(subscriber: QuerySubscriber,
                                       demandControlSubscription: DemandControlSubscription,
                                       queryContext: QueryContext) extends QueryCompletionTracker {
  private val count = new AtomicLong(0)
  private val errors = new ConcurrentLinkedQueue[Throwable]()
  private val latch = new CountDownLatch(1)

  override def increment(): Long = {
    val newCount = count.incrementAndGet()
    Zombie.debug(s"Incremented ${getClass.getSimpleName}. New count: $newCount")
    newCount
  }

  override def decrement(): Long = {
    val newCount = count.decrementAndGet()
    Zombie.debug(s"Decremented ${getClass.getSimpleName}. New count: $newCount")
    if (newCount == 0) {
      latch.countDown()
      val statistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())
      subscriber.onResultCompleted(statistics)
      demandControlSubscription.setCompleted()
    }
    else if (newCount < 0)
      throw new IllegalStateException("Cannot count below 0")
    newCount
  }

  override def error(throwable: Throwable): Unit = {
    errors.add(throwable)
    subscriber.onError(throwable)
    latch.countDown()
  }

  override def await(): Unit = {
    latch.await()
    if (!errors.isEmpty) {
      val firstException = errors.poll()
      errors.forEach(t => firstException.addSuppressed(t))
      throw firstException
    }
  }

  override def isCompleted: Boolean = latch.getCount == 0 || demandControlSubscription.isCompleteOrCancelled

  override def toString: String = s"ConcurrentQueryCompletionTracker(${count.get()})"
}
