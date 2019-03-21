/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

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
    * Await query completion. Blocks until the query has completed.
    */
  def await()

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
class StandardQueryCompletionTracker extends QueryCompletionTracker {
  private var count = 0L

  override def increment(): Long = {
    count += 1
    count
  }

  override def decrement(): Long = {
    count -= 1
    count
  }

  override def await(): Unit = {
    if (count != 0)
      throw new IllegalStateException(s"Should not reach await until tracking is complete! count: $count")
  }

  override def isCompleted: Boolean = count == 0

  override def toString: String = s"StandardQueryCompletionTracker($count)"
}

/**
  * Concurrent implementation of [[QueryCompletionTracker]].
  */
class ConcurrentQueryCompletionTracker extends QueryCompletionTracker {
  private val count = new AtomicLong(0)
  private val latch = new CountDownLatch(1)

  override def increment(): Long = {
    val newCount = count.incrementAndGet()
    newCount
  }

  override def decrement(): Long = {
    val newCount = count.decrementAndGet()
    if (newCount == 0)
      latch.countDown()
    if (newCount < 0)
      throw new IllegalStateException("Cannot count below 0")
    newCount
  }

  override def await(): Unit =
    latch.await()

  override def isCompleted: Boolean = count.get() == 0

  override def toString: String = s"ConcurrentQueryCompletionTracker(${count.get()})"
}
