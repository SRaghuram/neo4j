/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.{SchedulingResult, Task}

/**
  * Policy which selects the next task for execute for a given executing query.
  */
trait SchedulingPolicy {
  def nextTask(executingQuery: ExecutingQuery,
               queryResources: QueryResources): SchedulingResult[Task[QueryResources]]
}

object LazyScheduling extends SchedulingPolicy {

  def nextTask(executingQuery: ExecutingQuery,
               queryResources: QueryResources): SchedulingResult[Task[QueryResources]] = {

    // TODO this schedules RHS of hash join first. Not so good.
    val pipelineStates = executingQuery.executionState.pipelineStates

    val cleanUpTask = executingQuery.executionState.cleanUpTask()
    if (cleanUpTask != null) {
      return SchedulingResult(cleanUpTask, someTaskWasFilteredOut = false)
    }

    var i = pipelineStates.length - 1
    var someTaskWasFilteredOut = false
    while (i >= 0) {
      val pipelineState = pipelineStates(i)
      DebugSupport.SCHEDULING.log("[nextTask] probe pipeline (%s)", pipelineState.pipeline)
      val schedulingResult = pipelineState.nextTask(executingQuery.queryContext, executingQuery.queryState, queryResources)
      if (schedulingResult.task != null) {
        DebugSupport.SCHEDULING.log("[nextTask] schedule %s", schedulingResult)
        return schedulingResult
      } else if (schedulingResult.someTaskWasFilteredOut) {
        someTaskWasFilteredOut = true
      }
      i -= 1
    }
    SchedulingResult(null, someTaskWasFilteredOut)
  }
}
