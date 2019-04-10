/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.zombie.{ExecutablePipeline, ExecutionState, PipelineState}
import org.neo4j.util.Preconditions

/**
  * Grouping of all pipelines of an execution.
  * Used to compute their states before initializing the [[ExecutionState]].
  */
class PipelineExecutions(pipelines: IndexedSeq[ExecutablePipeline],
                         executionState: ExecutionState,
                         queryContext: QueryContext,
                         queryState: QueryState,
                         resources: QueryResources) {

  private val pipelineStates = {
    val states = new Array[PipelineState](pipelines.length)
    var i = 0
    while (i < states.length) {
      Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")
      states(i) = pipelines(i).createState(executionState, queryContext, queryState, resources)
      i += 1
    }
    states
  }

  def reverseIterator: Iterator[PipelineState] = pipelineStates.reverseIterator
}
