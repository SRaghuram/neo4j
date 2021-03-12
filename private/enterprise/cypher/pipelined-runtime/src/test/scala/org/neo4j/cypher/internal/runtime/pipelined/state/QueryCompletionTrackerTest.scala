/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber

class StandardQueryCompletionTrackerTest extends QueryCompletionTrackerTest(false) {
  override def newTracker(): QueryCompletionTracker = new StandardQueryCompletionTracker(subscriber,
    tracer,
    resources)
}

class StandardDebugQueryCompletionTrackerTest extends QueryCompletionTrackerTest(false) {
  override def newTracker(): QueryCompletionTracker = new StandardDebugQueryCompletionTracker(subscriber,
    tracer,
    resources)
}

class ConcurrentQueryCompletionTrackerTest extends QueryCompletionTrackerTest(true) {
  override def newTracker(): QueryCompletionTracker = new ConcurrentQueryCompletionTracker(subscriber,
    queryContext,
    tracer,
    resources)
}

class ConcurrentDebugQueryCompletionTrackerTest extends QueryCompletionTrackerTest(true) {
  override def newTracker(): QueryCompletionTracker = new ConcurrentDebugQueryCompletionTracker(subscriber,
    queryContext,
    tracer,
    resources)
}

abstract class QueryCompletionTrackerTest(shouldThawLocks: Boolean) extends CypherFunSuite {

  protected var subscriber: QuerySubscriber = _
  protected var queryContext: QueryContext = _
  protected var tracer: QueryExecutionTracer = _
  protected var transaction: KernelTransaction = _
  protected var resources: QueryResources = _
  protected val stats = QueryStatistics()

  def newTracker(): QueryCompletionTracker

  override protected def beforeEach(): Unit = {
    subscriber = mock[QuerySubscriber]
    queryContext = mock[QueryContext](RETURNS_DEEP_STUBS)
    tracer = mock[QueryExecutionTracer]
    transaction = mock[KernelTransaction]
    resources = mock[QueryResources](RETURNS_DEEP_STUBS)

    val txContext = mock[QueryTransactionalContext]
    when(queryContext.getOptStatistics).thenReturn(Some(stats))
    when(queryContext.transactionalContext).thenReturn(txContext)
    when(txContext.transaction).thenReturn(transaction)
  }

  private val trackerKey = QueryTrackerKey("test")
  test("should behave correctly on query completion") {
    val x = newTracker()

    // when
    x.increment(trackerKey)
    x.decrement(trackerKey)

    // then
    verify(subscriber).onResultCompleted(any[QueryStatistics])
    verify(subscriber, never()).onError(any())
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.await() shouldBe false
    x.hasEnded shouldBe true
    x.hasSucceeded shouldBe true
  }

  test("hasSucceeded should be true if cancelled after finished") {
    val x = newTracker()

    // when
    x.increment(trackerKey)
    x.decrement(trackerKey)
    x.await()
    x.cancel()

    // then
    x.hasEnded shouldBe true
    x.hasSucceeded shouldBe true
  }

  test("should behave correctly on query cancel") {
    val x = newTracker()

    // when
    x.increment(trackerKey)
    x.cancel()

    // then
    verify(subscriber, never()).onError(any())
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer, never()).stopQuery()
    verify(transaction, never()).thawLocks()
    x.hasEnded shouldBe false

    // when
    x.decrement(trackerKey) // The clean shutdown happens automatically when all tasks finished after a call to cancel

    // then
    verify(subscriber, never()).onResultCompleted(any())
    verify(subscriber, never()).onError(any())
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.await() shouldBe false
    x.hasEnded shouldBe true
    x.hasSucceeded shouldBe false
  }

  test("should behave correctly if query has no demand") {
    val x = newTracker()

    // when
    x.increment(trackerKey)
    x.request(1)
    x.addServed(1)

    // then
    verify(subscriber, never()).onResultCompleted(stats)
    verify(subscriber, never()).onError(any())
    verify(tracer, never()).stopQuery()
    verify(transaction, never()).thawLocks()
    x.await() shouldBe true
    x.hasEnded shouldBe false
    x.hasSucceeded shouldBe false
  }

  test("should behave correctly if query has failed") {
    val x = newTracker()
    val exception = new IllegalArgumentException

    // when
    x.increment(trackerKey)
    x.error(exception)

    // then
    verify(subscriber, never()).onError(exception)
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer, never()).stopQuery()
    verify(transaction, never()).thawLocks()
    x.hasEnded shouldBe false
    x.hasSucceeded shouldBe false

    // when
    x.decrement(trackerKey)

    // then
    verify(subscriber).onError(exception)
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.hasEnded shouldBe true
    x.hasSucceeded shouldBe false
    intercept[IllegalArgumentException] {
      x.await()
    }
  }

  test("should cleanly deal with reference counting bug") {
    val x = newTracker()

    // when
    x.increment(trackerKey)
    x.decrementBy(trackerKey, 2) // whoopsie

    // then
    val errorCaptor = argCaptor[Throwable]
    verify(subscriber).onError(errorCaptor.capture())
    errorCaptor.getValue shouldBe a[ReferenceCountingException]
    verify(subscriber, never()).onResultCompleted(stats)
    verify(tracer).stopQuery()
    verify(transaction, if (shouldThawLocks) times(1) else never()).thawLocks()
    x.hasEnded shouldBe true
    x.hasSucceeded shouldBe false
    intercept[ReferenceCountingException] {
      x.await()
    }
  }

  test("isCompleted when not completed") {
    val x = newTracker()

    // when
    x.increment(trackerKey)

    // then
    x.hasEnded shouldBe false
    x.hasSucceeded shouldBe false
  }
}
