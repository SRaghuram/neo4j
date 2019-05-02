/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.neo4j.kernel.impl.query.QuerySubscription

// TODO maybe we can have a thread-unsafe version too, for single threaded
class DemandControlSubscription extends QuerySubscription {
  private val demand = new AtomicLong(0)
  private val completed = new AtomicBoolean(false)
  private val cancelled = new AtomicBoolean(false)

  def getDemand: Long = demand.get()

  def hasDemand: Boolean = getDemand > 0

  def addServed(newlyServed: Long): Long =
    demand.addAndGet(-newlyServed)

  def isCompleteOrCancelled: Boolean = completed.get() || cancelled.get()

  def isCancelled: Boolean = cancelled.get()

  def setCompleted(): Unit = completed.set(true)

  // -------- Subscription Methods --------

  override def request(numberOfRecords: Long): Unit = {
    // numberOfRecords == newVal
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
      Thread.sleep(1000)
      completeOrCancelled = isCompleteOrCancelled
    }
    !completeOrCancelled
  }
}
