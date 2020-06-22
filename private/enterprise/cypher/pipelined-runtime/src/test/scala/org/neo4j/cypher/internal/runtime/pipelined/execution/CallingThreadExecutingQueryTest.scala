/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.InterpretedHead
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.runtime.pipelined.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.MockHelper.pipelineState
import org.neo4j.cypher.internal.runtime.pipelined.PipelineTask
import org.neo4j.cypher.internal.runtime.pipelined.SchedulingResult
import org.neo4j.cypher.internal.runtime.pipelined.Sleeper
import org.neo4j.cypher.internal.runtime.pipelined.Task
import org.neo4j.cypher.internal.runtime.pipelined.Worker
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.tracing.QueryExecutionTracer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CallingThreadExecutingQueryTest extends CypherFunSuite {
  private val idGen = new SequentialIdGen()

  test("should stop loop if there is no more work and nothing cancelled") {
    val executionState = getExecutionState(SchedulingResult(null, someTaskWasFilteredOut = false))

    val executingQuery = getExecutingQuery(executionState)

    // When
    executingQuery.request(Long.MaxValue)

    // Then no exception expected
  }

  test("should not stop loop if there is no more work but something cancelled") {
    val executionState = getExecutionState(SchedulingResult(null, someTaskWasFilteredOut = true))

    val executingQuery = getExecutingQuery(executionState)

    // When & Then
    a[SecondCallException] should be thrownBy {
      executingQuery.request(Long.MaxValue)
    }
  }

  test("should not stop loop if there is more work") {
    val task = mockTask
    val executionState = getExecutionState(SchedulingResult(task, someTaskWasFilteredOut = false))

    val executingQuery = getExecutingQuery(executionState)

    // When & Then
    a[SecondCallException] should be thrownBy {
      executingQuery.request(Long.MaxValue)
    }
    verify(executionState, never()).failQuery(any[Throwable], any[QueryResources], any[ExecutablePipeline])
    verify(task, times(1)).executeWorkUnit(null, null, null)
  }

  def getWorker: Worker = {
    new Worker(0, mock[QueryManager], mock[Sleeper]) {
      // Overridden to not swallow any exceptions
      override protected[pipelined] def scheduleNextTask(executingQuery: ExecutingQuery,
                                                         resources: QueryResources): SchedulingResult[Task[QueryResources]] =
        executingQuery.querySchedulingPolicy.nextTask(resources)

      override protected[pipelined] def executeTask(executingQuery: ExecutingQuery,
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
    val executionGraphDefinition = ExecutionGraphDefinition(
      null, null, null, Array(PipelineDefinition(PipelineId(0), PipelineId.NO_PIPELINE, PipelineId.NO_PIPELINE, InterpretedHead(Argument()(idGen)), mock[BufferDefinition], null, null, serial = false, None)), null
    )

    new CallingThreadExecutingQuery(
      executionState,
      getQueryState,
      mock[QueryExecutionTracer],
      mock[WorkersQueryProfiler],
      getWorker,
      mock[WorkerResourceProvider],
      new LazyExecutionGraphScheduling(executionGraphDefinition)
    )
  }

  private def getExecutionState(schedulingResults: SchedulingResult[PipelineTask]*) = {
    val m = mock[ExecutionState]
    when(m.hasEnded).thenReturn(false)

    val states = schedulingResults.map {
      r => pipelineState(PipelineId(0), schedulingResults = Seq(_ => r, _ => throw new SecondCallException()))
    }.toArray

    when(m.pipelineStates).thenReturn(states).thenThrow(new SecondCallException())
    m
  }

  private def getQueryState: PipelinedQueryState = {
    val m = mock[PipelinedQueryState]
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
