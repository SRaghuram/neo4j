/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue

// TODO maybe we can have a thread-unsafe version too, for single threaded
class ZombieSubscriber(subscriber: QuerySubscriber, visitor: QueryResultVisitor[_])
  extends QuerySubscription
    with QuerySubscriber
    with QueryResultVisitor[Exception] {

  private val demand = new AtomicLong(0)
  private val completed = new AtomicBoolean(false)
  private val cancelled = new AtomicBoolean(false)

  def getDemand: Long = demand.get()

  def hasDemand: Boolean = getDemand > 0

  def addServed(newlyServed: Long): Long =
    demand.addAndGet(-newlyServed)

  def isCompleteOrCancelled: Boolean = completed.get() || cancelled.get()

  def isCancelled: Boolean = cancelled.get()

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

  // -------- Subscriber Methods --------

  override def onResult(numberOfFields: Int): Unit =
    subscriber.onResult(numberOfFields)

  override def onRecord(): Unit =
    subscriber.onRecord()

  override def onField(offset: Int, value: AnyValue): Unit =
    subscriber.onField(offset, value)

  override def onRecordCompleted(): Unit =
    subscriber.onRecordCompleted()

  override def onError(throwable: Throwable): Unit =
    subscriber.onError(throwable)

  override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
    subscriber.onResultCompleted(statistics)
    completed.set(true)
  }

  override def visit(row: QueryResult.Record): Boolean = {
    visitor.visit(row)
  }
}
