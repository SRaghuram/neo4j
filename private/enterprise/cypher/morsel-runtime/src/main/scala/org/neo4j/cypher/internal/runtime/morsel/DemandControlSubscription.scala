/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.neo4j.kernel.impl.query.QuerySubscription

trait DemandControlSubscription  extends QuerySubscription {
  def getDemand: Long
  def hasDemand: Boolean
  def addServed(newlyServed: Long): Unit
  def isCompleteOrCancelled: Boolean
  def isCancelled: Boolean
  def setCompleted(): Unit
}

class StandardDemandControlSubscription extends DemandControlSubscription {
  private var demand = 0L
  private var completed = false
  private var cancelled = false

  def getDemand: Long = demand

  def hasDemand: Boolean = getDemand > 0

  def addServed(newlyServed: Long): Unit = {
    demand -= newlyServed
  }

  def isCompleteOrCancelled: Boolean = completed || cancelled

  def isCancelled: Boolean = cancelled

  def setCompleted(): Unit = {
    completed = true
  }

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
    if (demand > 0) {
      throw new IllegalStateException("When single-threaded await should not be called before demand has been served")
    }
    !isCompleteOrCancelled
  }
}

class ConcurrentDemandControlSubscription extends DemandControlSubscription {
  private val demand = new AtomicLong(0)
  private val completed = new AtomicBoolean(false)
  private val cancelled = new AtomicBoolean(false)

  def getDemand: Long = demand.get()

  def hasDemand: Boolean = getDemand > 0

  def addServed(newlyServed: Long): Unit =
    demand.addAndGet(-newlyServed)

  def isCompleteOrCancelled: Boolean = completed.get() || cancelled.get()

  def isCancelled: Boolean = cancelled.get()

  def setCompleted(): Unit = completed.set(true)

  // -------- Subscription Methods --------

  override def request(numberOfRecords: Long): Unit = {
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

  override def cancel(): Unit = cancelled.set(true)

  override def await(): Boolean = {
    var completeOrCancelled = isCompleteOrCancelled
    while (demand.get() > 0 && !completeOrCancelled) {
      // TODO reviewer: is this sleep too long?
      Thread.sleep(10)
      completeOrCancelled = isCompleteOrCancelled
    }
    !completeOrCancelled
  }
}
