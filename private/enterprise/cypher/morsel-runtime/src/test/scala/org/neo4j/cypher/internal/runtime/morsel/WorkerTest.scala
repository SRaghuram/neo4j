/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class WorkerTest extends CypherFunSuite {

  test("should not reset sleeper when no queries") {

    val ATTEMPTS = 100
    val countDown = new CountDown[ExecutingQuery](ATTEMPTS, mockExecutingQuery)
    val queryManager: QueryManager =
      new QueryManager {
        override def nextQueryToWorkOn(workerId: Int): ExecutingQuery = countDown.next()
      }
    val schedulingPolicy: SchedulingPolicy = (_: ExecutingQuery, _: QueryResources) => null
    val sleeper = mock[Sleeper]

    val worker = new Worker(1, queryManager, schedulingPolicy, sleeper)
    countDown.worker = worker
    worker.run()

    verify(sleeper, never()).reportStopWorkUnit()
    verify(sleeper, times(ATTEMPTS)).reportIdle()
  }

  test("should not reset sleeper when query has with no work") {
    val ATTEMPTS = 100
    val queryManager: QueryManager =
      new QueryManager {
        override def nextQueryToWorkOn(workerId: Int): ExecutingQuery = mockExecutingQuery
      }

    val countDown = new CountDown[PipelineTask](ATTEMPTS, null)
    val schedulingPolicy: SchedulingPolicy = (_: ExecutingQuery, _: QueryResources) => countDown.next()

    val sleeper = mock[Sleeper]
    val worker = new Worker(1, queryManager, schedulingPolicy,  sleeper)
    countDown.worker = worker
    worker.run()

    verify(sleeper, never()).reportStopWorkUnit()
    verify(sleeper, times(ATTEMPTS)).reportIdle()
  }

  test("should handle scheduling error which occurred before getting morsel from parallelizer") {
    val cause = new Exception
    val originalMorsel = mock[MorselExecutionContext]
    val input = mock[MorselParallelizer]
    when(input.originalForClosing).thenReturn(originalMorsel)
    val query = mock[ExecutingQuery]
    val executionState = mock[ExecutionState]
    when(query.executionState).thenReturn(executionState)
    val pipeline = mock[ExecutablePipeline]
    val resources = mock[QueryResources]

    val schedulingPolicy: SchedulingPolicy =
      (_: ExecutingQuery, _: QueryResources) =>
        throw NextTaskException(pipeline, SchedulingInputException(input, cause))

    val worker = new Worker(1, mock[QueryManager], schedulingPolicy, mock[Sleeper])
    worker.scheduleNextTask(query, resources) shouldBe null

    verify(input).originalForClosing
    verify(input, never()).nextCopy
    verify(executionState).closeMorselTask(pipeline, originalMorsel)
    verify(executionState).failQuery(cause, resources, pipeline)
  }

  test("should handle scheduling error which occurred after getting morsel from parallelizer") {
    val cause = new Exception
    val originalMorsel = mock[MorselExecutionContext]
    val input = mock[MorselParallelizer]
    when(input.originalForClosing).thenReturn(originalMorsel)
    val query = mock[ExecutingQuery]
    val executionState = mock[ExecutionState]
    when(query.executionState).thenReturn(executionState)
    val pipeline = mock[ExecutablePipeline]
    val resources = mock[QueryResources]

    val schedulingPolicy: SchedulingPolicy =
      (_: ExecutingQuery, _: QueryResources) =>
        throw NextTaskException(pipeline, SchedulingInputException(input, cause))

    val worker = new Worker(1, mock[QueryManager], schedulingPolicy, mock[Sleeper])
    worker.scheduleNextTask(query, resources) shouldBe null

    verify(input).originalForClosing
    verify(input, never()).nextCopy
    verify(executionState).closeMorselTask(pipeline, originalMorsel)
    verify(executionState).failQuery(cause, resources, pipeline)
  }

  class NoMoreAttemptsException() extends IllegalArgumentException

  class CountDown[T](var count: Int, val t: T) {
    var worker: Worker = _
    def next(): T = {
      count -= 1
      if (count == 0)
        worker.stop()
      t
    }
  }

  private def mockExecutingQuery = {
    val executingQuery = mock[ExecutingQuery]
    val provider = mock[WorkerResourceProvider]
    when(executingQuery.workerResourceProvider).thenReturn(provider)
    when(provider.resourcesForWorker(anyInt())).thenReturn(null)

    executingQuery
  }
}
