/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import java.util

import org.eclipse.collections.impl.factory.primitive.IntStacks
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.{ExecutionGraphDefinition, PipelineId}
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.{SchedulingResult, Task}

object LazyScheduling extends SchedulingPolicy {
  def executionGraphSchedulingPolicy(executionGraphDefinition: ExecutionGraphDefinition): ExecutionGraphSchedulingPolicy = {
    new LazyExecutionGraphScheduling(executionGraphDefinition)
  }
}

class LazyExecutionGraphScheduling(executionGraphDefinition: ExecutionGraphDefinition) extends ExecutionGraphSchedulingPolicy {
  // The pipelines in the correct order for this scheduling policy
  private[execution] val pipelinesInLHSDepthFirstOrder: Array[PipelineId] = {
    val pipelinesInExecutionOrder = executionGraphDefinition.pipelines
    val result = new Array[PipelineId](pipelinesInExecutionOrder.length)

    val stack =  IntStacks.mutable.empty()
    val visited = new util.BitSet(pipelinesInExecutionOrder.length)
    var i = 0

    stack.push(pipelinesInExecutionOrder.length - 1)

    while (stack.notEmpty()) {
      val pipelineId = stack.pop()
      if(!visited.get(pipelineId)) {
        val pipelineState = pipelinesInExecutionOrder(pipelineId)

        result(i) = PipelineId(pipelineId)
        i += 1

        visited.set(pipelineId)
        if (pipelineState.rhs != NO_PIPELINE) {
          stack.push(pipelineState.rhs.x)
        }
        if (pipelineState.lhs != NO_PIPELINE) {
          stack.push(pipelineState.lhs.x)
        }
      }
    }

    result
  }

  override def querySchedulingPolicy(executingQuery: ExecutingQuery): QuerySchedulingPolicy = new LazyQueryScheduling(executingQuery, pipelinesInLHSDepthFirstOrder)
}

class LazyQueryScheduling(executingQuery: ExecutingQuery, pipelinesInLHSDepthFirstOrder: Array[PipelineId]) extends QuerySchedulingPolicy {

  def nextTask(queryResources: QueryResources): SchedulingResult[Task[QueryResources]] = {
    val pipelineStates = executingQuery.executionState.pipelineStates

    val cleanUpTask = executingQuery.executionState.cleanUpTask()
    if (cleanUpTask != null) {
      return SchedulingResult(cleanUpTask, someTaskWasFilteredOut = false)
    }

    var i = 0
    var someTaskWasFilteredOut = false
    while (i < pipelinesInLHSDepthFirstOrder.length) {
      val pipelineState = pipelineStates(pipelinesInLHSDepthFirstOrder(i).x)
      DebugSupport.SCHEDULING.log("[nextTask] probe pipeline (%s)", pipelineState.pipeline)
      val schedulingResult = pipelineState.nextTask(executingQuery.queryContext, executingQuery.queryState, queryResources)
      if (schedulingResult.task != null) {
        DebugSupport.SCHEDULING.log("[nextTask] schedule %s", schedulingResult)
        return schedulingResult
      } else if (schedulingResult.someTaskWasFilteredOut) {
        someTaskWasFilteredOut = true
      }
      i += 1
    }
    SchedulingResult(null, someTaskWasFilteredOut)
  }
}
