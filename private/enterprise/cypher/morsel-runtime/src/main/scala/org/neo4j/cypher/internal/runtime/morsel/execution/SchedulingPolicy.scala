/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.Task

/**
  * Policy which selects the next task for execute for a given executing query.
  */
trait SchedulingPolicy {
  def nextTask(executingQuery: ExecutingQuery,
               queryResources: QueryResources): Task[QueryResources]
}

object LazyScheduling extends SchedulingPolicy {

  def nextTask(executingQuery: ExecutingQuery,
               queryResources: QueryResources): Task[QueryResources] = {

    // TODO this schedules RHS of hash join first. Not so good.
    val pipelineStates = executingQuery.executionState.pipelineStates

    val cleanUpTask = executingQuery.executionState.cleanUpTask()
    if (cleanUpTask != null) {
      return cleanUpTask
    }

    var i = pipelineStates.length - 1
    while (i >= 0) {
      val pipelineState = pipelineStates(i)
      DebugSupport.SCHEDULING.log("[nextTask] probe pipeline (%s)", pipelineState.pipeline)
      val task = pipelineState.nextTask(executingQuery.queryContext, executingQuery.queryState, queryResources)
      if (task != null) {
        DebugSupport.SCHEDULING.log("[nextTask] schedule %s", task)
        return task
      }
      i -= 1
    }
    null
  }
}
