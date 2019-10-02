/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class CallingThreadExecutingQueryTest extends CypherFunSuite {

  test("should stop loop if there is no more work and nothing cancelled") {
    val executionState = getExecutionState( 1,
      SchedulingResult(null, someTaskWasFilteredOut = false)
    )

    val executingQuery = getExecutingQuery(executionState)

    // When
    executingQuery.request(Long.MaxValue)

    // Then no exception expected
  }

  test("should not stop loop if there is no more work but something cancelled") {
    val executionState = getExecutionState( 1,
      SchedulingResult(null, someTaskWasFilteredOut = true)
    )

    val executingQuery = getExecutingQuery(executionState)

    // When & Then
    a[SecondCallException] should be thrownBy {
      executingQuery.request(Long.MaxValue)
    }
  }

  test("should not stop loop if there is more work") {
    val task = mockTask
    val executionState = getExecutionState( 1,
      SchedulingResult(task, someTaskWasFilteredOut = false)
    )

    val executingQuery = getExecutingQuery(executionState)

    // When & Then
    a[SecondCallException] should be thrownBy {
      executingQuery.request(Long.MaxValue)
    }
    verify(executionState, never()).failQuery(any[Throwable], any[QueryResources], any[ExecutablePipeline])
    verify(task, times(1)).executeWorkUnit(null, null, null)
  }

  def pipelineStates(schedulingResults: SchedulingResult[PipelineTask]*): Array[PipelineState] = {
    schedulingResults.map {
      r =>
        val m = mock[PipelineState]
        when(m.nextTask(any[QueryContext], any[QueryState], any[QueryResources])).thenReturn(r)
        m
    }.toArray
  }

  def getWorker: Worker = {
    new Worker(0, mock[QueryManager], LazyScheduling, mock[Sleeper]) {
      // Overridden to not swallow any exceptions
      override protected[morsel] def scheduleNextTask(executingQuery: ExecutingQuery,
                                                      resources: QueryResources): SchedulingResult[Task[QueryResources]] =
        schedulingPolicy.nextTask(executingQuery, resources)

      override protected[morsel] def executeTask(executingQuery: ExecutingQuery,
                                                 task: PipelineTask,
                                                 resources: QueryResources): Unit = {
        // Simplifying the actual implementation, but leaving traces of work done that can be verified
        task.executeWorkUnit(null, null, null)
      }
    }
  }

  def mockTask: PipelineTask = {
    val m = mock[PipelineTask]
    doReturn(null, Nil: _*).when(m).executeWorkUnit(null, null, null)
    m
  }

  def getExecutingQuery(executionState: ExecutionState): CallingThreadExecutingQuery = {
    new CallingThreadExecutingQuery(
      executionState,
      mock[QueryContext],
      getQueryState,
      mock[QueryExecutionTracer],
      mock[WorkersQueryProfiler],
      getWorker,
      mock[WorkerResourceProvider]
    )
  }

  private def getExecutionState(iterations: Int, schedulingResults: SchedulingResult[PipelineTask]*) = {
    val m = mock[ExecutionState]
    when(m.hasEnded).thenReturn(false)
    val states = pipelineStates(schedulingResults: _*)
    when(m.pipelineStates).thenReturn(states).thenThrow(new SecondCallException())
    m
  }

  private def getQueryState: QueryState = {
    val m = mock[QueryState]
    val flowControl = getFlowControl
    when(m.flowControl).thenReturn(flowControl)
    m
  }

  private def getFlowControl: FlowControl = {
    val m = mock[FlowControl]
    when(m.hasDemand).thenReturn(true)
    m
  }

  class SecondCallException extends RuntimeException("Did not expect this method to be called twice.")
}
