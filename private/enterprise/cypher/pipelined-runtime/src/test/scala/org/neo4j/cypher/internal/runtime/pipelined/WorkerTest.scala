/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.pipelined.execution.ExecutingQuery
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryManager
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QuerySchedulingPolicy
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class WorkerTest extends CypherFunSuite {

  test("should not reset sleeper when no queries") {

    val ATTEMPTS = 100
    val schedulingPolicy: QuerySchedulingPolicy = (_: QueryResources) => SchedulingResult(null, someTaskWasFilteredOut = false)
    val countDown = new CountDown[ExecutingQuery](ATTEMPTS, mockExecutingQuery(schedulingPolicy))
    val queryManager: QueryManager =
      new QueryManager {
        override def nextQueryToWorkOn(workerId: Int): ExecutingQuery = countDown.next()
      }

    val sleeper = mock[Sleeper]

    val worker = new Worker(1, queryManager, sleeper)
    countDown.worker = worker
    worker.run()

    verify(sleeper, never()).reportStopWorkUnit()
    verify(sleeper, times(ATTEMPTS)).reportIdle()
  }

  test("should not reset sleeper when query has with no work") {
    val ATTEMPTS = 100
    val countDown = new CountDown[PipelineTask](ATTEMPTS, null)
    val schedulingPolicy: QuerySchedulingPolicy = (_: QueryResources) => SchedulingResult(countDown.next(), someTaskWasFilteredOut = false)
    val queryManager: QueryManager =
      new QueryManager {
        override def nextQueryToWorkOn(workerId: Int): ExecutingQuery = mockExecutingQuery(schedulingPolicy)
      }


    val sleeper = mock[Sleeper]
    val worker = new Worker(1, queryManager, sleeper)
    countDown.worker = worker
    worker.run()

    verify(sleeper, never()).reportStopWorkUnit()
    verify(sleeper, times(ATTEMPTS)).reportIdle()
  }

  test("should handle scheduling error which occurred after getting morsel from parallelizer") {
    val cause = new Exception
    val originalMorsel = mock[Morsel]
    val input = mock[MorselParallelizer]
    val pipeline = mock[ExecutablePipeline]
    when(input.originalForClosing).thenReturn(originalMorsel)

    val schedulingPolicy: QuerySchedulingPolicy =
      (_: QueryResources) =>
        throw NextTaskException(pipeline, SchedulingInputException(input, cause))

    val query = mockExecutingQuery(schedulingPolicy)
    val executionState = mock[ExecutionState]
    when(query.executionState).thenReturn(executionState)
    val resources = mock[QueryResources]


    val worker = new Worker(1, mock[QueryManager], mock[Sleeper])
    worker.scheduleNextTask(query, resources) shouldBe SchedulingResult(null, someTaskWasFilteredOut = true)

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

  private def mockExecutingQuery(qsp: QuerySchedulingPolicy) = {
    val executingQuery = mock[ExecutingQuery]
    val provider = mock[WorkerResourceProvider]
    when(executingQuery.workerResourceProvider).thenReturn(provider)
    when(provider.resourcesForWorker(anyInt())).thenReturn(null)
    when(executingQuery.querySchedulingPolicy).thenReturn(qsp)

    executingQuery
  }
}
