/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.eclipse.collections.impl.factory.primitive.IntStacks
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStreamBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.SchedulingResult
import org.neo4j.cypher.internal.runtime.pipelined.Task
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory

object LazyScheduling extends SchedulingPolicy {
  def executionGraphSchedulingPolicy(executionGraphDefinition: ExecutionGraphDefinition): ExecutionGraphSchedulingPolicy = {
    new LazyExecutionGraphScheduling(executionGraphDefinition)
  }
}

class LazyExecutionGraphScheduling(executionGraphDefinition: ExecutionGraphDefinition) extends ExecutionGraphSchedulingPolicy {
  // The pipelines in the correct order for this scheduling policy
  private[execution] val (pipelinesInLHSDepthFirstOrder: Array[PipelineId], priority: Array[Boolean]) = {
    val pipelinesInExecutionOrder = executionGraphDefinition.pipelines
    val result = new Array[PipelineId](pipelinesInExecutionOrder.length)
    val priority = new Array[Boolean](pipelinesInExecutionOrder.length)

    val stack = IntStacks.mutable.empty()
    val visited = new Array[Boolean](pipelinesInExecutionOrder.length)
    var i = 0

    stack.push(pipelinesInExecutionOrder.length - 1)

    while (stack.notEmpty()) {
      val pipelineId = stack.pop()
      if(!visited(pipelineId)) {
        val pipelineState = pipelinesInExecutionOrder(pipelineId)
        result(i) = PipelineId(pipelineId)
        priority(i) =
          pipelineState.inputBuffer.variant match {
            case _: ArgumentStateBufferVariant
                 | _: ArgumentStreamBufferVariant => true
            case _ => pipelineState.headPlan.plan match {
              case _: ConditionalApply => true
              case _ => pipelineId == 0 // always give the leftmost leaf priority
            }
          }
        i += 1

        visited(pipelineId) = true
        if (pipelineState.rhs != NO_PIPELINE) {
          stack.push(pipelineState.rhs.x)
        }
        if (pipelineState.lhs != NO_PIPELINE) {
          stack.push(pipelineState.lhs.x)
        }
      }
    }

    (result, priority)
  }

  override def querySchedulingPolicy(executingQuery: ExecutingQuery, stateFactory: StateFactory): QuerySchedulingPolicy =
    new LazyQueryScheduling(executingQuery, pipelinesInLHSDepthFirstOrder, priority, stateFactory)
}

class LazyQueryScheduling(executingQuery: ExecutingQuery,
                          pipelinesInLHSDepthFirstOrder: Array[PipelineId],
                          priority: Array[Boolean],
                          stateFactory: StateFactory)
  extends LowMarkScheduling[Task[QueryResources]](stateFactory, priority) with QuerySchedulingPolicy {

  private[this] val pipelineStates = executingQuery.executionState.pipelineStates

  def nextTask(queryResources: QueryResources): SchedulingResult[Task[QueryResources]] = {
    val cleanUpTask = executingQuery.executionState.cleanUpTask()
    if (cleanUpTask != null) {
      return SchedulingResult(cleanUpTask, someTaskWasFilteredOut = false)
    }
    schedule(queryResources)
  }

  override protected def tryScheduleItem(i: Int,
                                         queryResources: QueryResources): SchedulingResult[Task[QueryResources]] = {

    val pipelineState = pipelineStates(pipelinesInLHSDepthFirstOrder(i).x)
    DebugSupport.SCHEDULING.log("[nextTask] probe pipeline (%s)", pipelineState.pipeline)
    pipelineState.nextTask(executingQuery.queryState, queryResources)
  }
}

abstract class LowMarkScheduling[T](stateFactory: StateFactory, priority: Array[Boolean]) {
  private[this] val n = priority.length
  private[this] val mark = stateFactory.newLowMark(n - 1)

  protected def schedule(queryResources: QueryResources): SchedulingResult[T] = {
    val priorityMark = mark.get()

    var item = 0
    var someTaskWasFilteredOut = false
    while (item < n) {
      if (item >= priorityMark || priority(item)) {
        val schedulingResult = tryScheduleItem(item, queryResources)
        if (schedulingResult.task != null) {
          DebugSupport.SCHEDULING.log("[nextTask] schedule %s", schedulingResult)
          if (item > 0)
            mark.setLowMark(item - 1)
          return schedulingResult
        } else if (schedulingResult.someTaskWasFilteredOut) {
          someTaskWasFilteredOut = true
        }
      }
      item += 1
    }
    SchedulingResult(null.asInstanceOf[T], someTaskWasFilteredOut)
  }

  protected def tryScheduleItem(i: Int, queryResources: QueryResources): SchedulingResult[T]
}
