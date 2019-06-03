/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal.runtime._
import org.neo4j.graphdb.{ExecutionPlanDescription, Notification}
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySubscriber}

/**
  * Ensures execution results are closed. This is tricky because we try to be smart about
  * closing results automatically when
  *
  *  1) all result rows have been seen through iterator
  *  2) all result rows have been seen through visitor
  *  3) all result rows have been seen through dumpToString
  *  4) any operator throws an exception
  *
  * In addition we also have special handling for suppressing exceptions thrown on close()
  * after responding to a 4).
  *
  * Finally this class report to the [[innerMonitor]] when the query is closed.
  *
  * @param query metadata about the executing query
  * @param inner the actual result
  * @param runSafely RunSafely which converts any exception into the public exception space (subtypes of org.neo4j.cypher.CypherException)
  * @param innerMonitor monitor to report closing of the query to
  */
class ClosingExecutionResult private(val query: ExecutingQuery,
                                     val inner: InternalExecutionResult,
                                     runSafely: RunSafely,
                                     innerMonitor: QueryExecutionMonitor,
                                     subscriber: QuerySubscriber) extends InternalExecutionResult {

  self =>

  private val monitor = OnlyOnceQueryExecutionMonitor(innerMonitor)

  override def initiate(): Unit = {
    safely { inner.initiate() }

    if (inner.isClosed)
      monitor.endSuccess(query)
  }

  override def fieldNames(): Array[String] = safely { inner.fieldNames() }

  override def executionPlanDescription(): ExecutionPlanDescription =
    safely {
      inner.executionPlanDescription()
    }

  override def close(reason: CloseReason): Unit = runSafely({
    inner.close(reason)
    reason match {
      case Success => monitor.endSuccess(query)
      case Failure => monitor.endFailure(query, null)
      case Error(t) => monitor.endFailure(query, t)
    }
  })( handleErrorOnClose(reason) )

  override def queryType: InternalQueryType = safely { inner.queryType }

  override def notifications: Iterable[Notification] = safely { inner.notifications }

  override def executionMode: ExecutionMode = safely { inner.executionMode }

  override def toString: String = runSafely { inner.toString }

  // HELPERS

  private def safely[T](body: => T): T = runSafely(body)(closeAndRethrowOnError)

  private def closeAndRethrowOnError[T](t: Throwable): T = {
    try {
      close(Error(t))
    } catch {
      case _: Throwable =>
        // ignore
    }
    throw t
  }

  private def closeAndCallOnError(t: Throwable): Unit = {
    try {
      subscriber.onError(t)
      close(Error(t))
    } catch {
      case _: Throwable =>
      // ignore
    }
  }

  private def handleErrorOnClose(closeReason: CloseReason)(thrownDuringClose: Throwable): Unit = {
    closeReason match {
      case Error(thrownBeforeClose) =>
        try {
          thrownBeforeClose.addSuppressed(thrownDuringClose)
        } catch {
          case _: Throwable => // Ignore
        }
        monitor.endFailure(query, thrownBeforeClose)

      case _ =>
        monitor.endFailure(query, thrownDuringClose)
    }
    throw thrownDuringClose
  }

  override def isClosed: Boolean = inner.isClosed

  override def request(numberOfRows: Long): Unit =
    runSafely(inner.request(numberOfRows))(closeAndCallOnError)

  override def cancel(): Unit = runSafely(inner.cancel())(closeAndCallOnError)

  override def await(): Boolean = {
    val hasMore = runSafely(inner.await())(e => {
      closeAndCallOnError(e)
      false
    })

    if (!hasMore) {
      close()
    }
    hasMore
  }
}

object ClosingExecutionResult {
  def wrapAndInitiate(query: ExecutingQuery,
                      inner: InternalExecutionResult,
                      runSafely: RunSafely,
                      innerMonitor: QueryExecutionMonitor,
                      subscriber: QuerySubscriber): ClosingExecutionResult = {

    val result = new ClosingExecutionResult(query, inner, runSafely, innerMonitor, subscriber)
    result.initiate()
    result
  }
}
