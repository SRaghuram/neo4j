/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import java.util

import org.eclipse.collections.impl.factory.primitive.IntStacks
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.{PipelineState, SchedulingResult, Task}

trait SchedulingPolicy {
  /**
   * Return a QuerySchedulingPolicy for the given query
   */
  def querySchedulingPolicy(executingQuery: ExecutingQuery): QuerySchedulingPolicy
}

/**
  * Policy which selects the next task to execute for a given executing query.
  */
trait QuerySchedulingPolicy {
  /**
   * Return the next task (together with the information if some task was cancelled), if there was any work to be done.
   * @param queryResources the query resources
   */
  def nextTask(queryResources: QueryResources): SchedulingResult[Task[QueryResources]]
}

class LazyQueryScheduling(executingQuery: ExecutingQuery) extends QuerySchedulingPolicy {

  // Initialize the pipeline states in the correct order for this scheduling policy
  private def initPipelineStatesInLHSDepthFirstOrder: Array[PipelineState] = {
    val pipelineStatesInExecutionOrder = executingQuery.executionState.pipelineStates
    val result = new Array[PipelineState](pipelineStatesInExecutionOrder.length)

    val stack =  IntStacks.mutable.empty()
    val visited = new util.BitSet(pipelineStatesInExecutionOrder.length)
    var i = 0

    stack.push(pipelineStatesInExecutionOrder.length - 1)

    while (stack.notEmpty()) {
      val pipelineId = stack.pop()
      if(!visited.get(pipelineId)) {
        val pipelineState = pipelineStatesInExecutionOrder(pipelineId)

        result(i) = pipelineState
        i += 1

        visited.set(pipelineId)
        if (pipelineState.pipeline.rhs != NO_PIPELINE) {
          stack.push(pipelineState.pipeline.rhs.x)
        }
        if (pipelineState.pipeline.lhs != NO_PIPELINE) {
          stack.push(pipelineState.pipeline.lhs.x)
        }
      }
    }

    result
  }

  private[execution] val pipelineStates = initPipelineStatesInLHSDepthFirstOrder

  def nextTask(queryResources: QueryResources): SchedulingResult[Task[QueryResources]] = {

    val cleanUpTask = executingQuery.executionState.cleanUpTask()
    if (cleanUpTask != null) {
      return SchedulingResult(cleanUpTask, someTaskWasFilteredOut = false)
    }

    var i = 0
    var someTaskWasFilteredOut = false
    while (i < pipelineStates.length) {
      val pipelineState = pipelineStates(i)
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

object LazyScheduling extends SchedulingPolicy {
  def querySchedulingPolicy(executingQuery: ExecutingQuery): QuerySchedulingPolicy = {
    new LazyQueryScheduling(executingQuery)
  }
}
