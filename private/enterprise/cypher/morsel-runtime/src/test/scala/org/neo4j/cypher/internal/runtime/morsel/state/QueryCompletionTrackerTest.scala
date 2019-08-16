/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics, QueryTransactionalContext}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.kernel.impl.query.QuerySubscriber

class StandardQueryCompletionTrackerTest extends QueryCompletionTrackerTest(false) {
  override def newTracker(): QueryCompletionTracker = new StandardQueryCompletionTracker(subscriber,
                                                                                         queryContext,
                                                                                         tracer)
}

class ConcurrentQueryCompletionTrackerTest extends QueryCompletionTrackerTest(true) {
  override def newTracker(): QueryCompletionTracker = new ConcurrentQueryCompletionTracker(subscriber,
                                                                                           queryContext,
                                                                                           tracer)
}

abstract class QueryCompletionTrackerTest(shouldThawLocks: Boolean) extends CypherFunSuite {

  protected var subscriber: QuerySubscriber = _
  protected var queryContext: QueryContext = _
  protected var tracer: QueryExecutionTracer = _
  protected var transaction: Transaction = _
  protected val stats = QueryStatistics()

  def newTracker(): QueryCompletionTracker

  override protected def beforeEach(): Unit = {
    subscriber = mock[QuerySubscriber]
    queryContext = mock[QueryContext](RETURNS_DEEP_STUBS)
    tracer = mock[QueryExecutionTracer]
    transaction = mock[Transaction]

    val txContext = mock[QueryTransactionalContext]
    when(queryContext.getOptStatistics).thenReturn(Some(stats))
    when(queryContext.transactionalContext).thenReturn(txContext)
    when(txContext.transaction).thenReturn(transaction)
  }

  test("should behave correctly on query completion") {
    val x = newTracker()

    // when
    x.increment()
    x.decrement()

    // then
    verify(subscriber).onResultCompleted(stats)
    verify(subscriber, never()).onError(any())
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.await() shouldBe false
    x.hasEnded shouldBe true
  }

  test("should behave correctly on query cancel") {
    val x = newTracker()

    // when
    x.increment()
    x.cancel()

    // then
    verify(subscriber, never()).onError(any())
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer, never()).stopQuery()
    verify(transaction, never()).thawLocks()
    x.hasEnded shouldBe false

    // when
    x.decrement() // The clean shutdown happens automatically when all tasks finished after a call to cancel

    // then
    verify(subscriber, never()).onResultCompleted(any())
    verify(subscriber, never()).onError(any())
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.await() shouldBe false
    x.hasEnded shouldBe true
  }

  test("should behave correctly if query has no demand") {
    val x = newTracker()

    // when
    x.increment()
    x.request(1)
    x.addServed(1)

    // then
    verify(subscriber, never()).onResultCompleted(stats)
    verify(subscriber, never()).onError(any())
    verify(tracer, never()).stopQuery()
    verify(transaction, never()).thawLocks()
    x.await() shouldBe true
    x.hasEnded shouldBe false
  }

  test("should behave correctly if query has failed") {
    val x = newTracker()
    val exception = new IllegalArgumentException

    // when
    x.increment()
    x.error(exception)

    // then
    verify(subscriber, never()).onError(exception)
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer, never()).stopQuery()
    verify(transaction, never()).thawLocks()
    x.hasEnded shouldBe false

    // when
    x.decrement()

    // then
    verify(subscriber).onError(exception)
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.hasEnded shouldBe true
    intercept[IllegalArgumentException] {
      x.await()
    }
  }

  test("isCompleted when not completed") {
    val x = newTracker()

    // when
    x.increment()

    // then
    x.hasEnded should be(false)
  }
}
