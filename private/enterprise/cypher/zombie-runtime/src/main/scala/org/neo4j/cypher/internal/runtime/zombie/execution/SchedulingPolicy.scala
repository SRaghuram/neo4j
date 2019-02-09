/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import org.neo4j.cypher.internal.runtime.morsel.QueryResources
import org.neo4j.cypher.internal.runtime.zombie.PipelineTask

/**
  * Policy which selects the next task for execute for a given executing query.
  */
trait SchedulingPolicy {
  def nextTask(executingQuery: ExecutingQuery,
               queryResources: QueryResources): PipelineTask
}

object LazyScheduling extends SchedulingPolicy {

  def nextTask(executingQuery: ExecutingQuery,
               queryResources: QueryResources): PipelineTask = {

    val state = executingQuery.executionState

    for (p <- executingQuery.executablePipelines.reverseIterator) {
      val task = state.continue(p)
      if (task != null) {
        return task
      }

      val input = state.consumeMorsel(p.inputRowBuffer.id, p)
      if (input != null) {
        val pipelineState = state.pipelineState(p.id)
        val tasks = pipelineState.init(input, executingQuery.queryContext, executingQuery.queryState, queryResources)
        for (task <- tasks.tail)
          state.addContinuation(task)
        return tasks.head
      }
    }
    null
  }
}
