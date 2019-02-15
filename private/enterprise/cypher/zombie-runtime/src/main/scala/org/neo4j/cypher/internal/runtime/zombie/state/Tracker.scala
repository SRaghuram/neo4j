/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.runtime.zombie.Zombie.debug

/**
  * A [[Tracker]] tracks the progress of some process. This is done by keeping an internal
  * count of events. When the count is zero the process has completed.
  */
trait Tracker {
  /**
    * Increment the tracker count.
    */
  def increment(): Long

  /**
    * Decrement the tracker count.
    */
  def decrement(): Long

  /**
    * Await process completion. Blocks until the process has completed.
    */
  def await()

  /**
    * Query process completion state. Non-blocking.
    *
    * @return true iff the process has completed
    */
  def isCompleted: Boolean
}

/**
  * Not thread-safe implementation of [[Tracker]].
  */
class StandardTracker extends Tracker {
  private var count = 0L

  override def increment(): Long = {
    count += 1
    count
  }

  override def decrement(): Long = {
    count -= 1
    count
  }

  override def await(): Unit =
    if (count != 0)
      throw new IllegalStateException("Should not reach await until tracking is complete!")

  override def isCompleted: Boolean = count == 0
}

/**
  * Concurrent implementation of [[Tracker]].
  */
class ConcurrentTracker extends Tracker {
  private var count = new AtomicLong(0)
  private var latch = new CountDownLatch(1)

  override def increment(): Long = {
    val newCount = count.incrementAndGet()
//    debug("incr TRACKER to %d".format(newCount))
    newCount
  }

  override def decrement(): Long = {
    val newCount = count.decrementAndGet()
//    debug("decr TRACKER to %d".format(newCount))
    if (newCount == 0)
      latch.countDown()
    if (newCount < 0)
      throw new IllegalStateException("Cannot count below 0")
    newCount
  }

  override def await(): Unit =
    latch.await()

  override def isCompleted: Boolean = count.get() == 0
}
