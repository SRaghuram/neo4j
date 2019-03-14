/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.zombie.{ExecutablePipeline, ExecutionState, PipelineState}
import org.neo4j.util.Preconditions

class PipelineExecutions(pipelines: Seq[ExecutablePipeline],
                         executionState: ExecutionState,
                         queryContext: QueryContext,
                         queryState: QueryState,
                         resources: QueryResources) {

  for (i <- pipelines.indices)
    Preconditions.checkState(i == pipelines(i).id.x, "Pipeline id does not match offset!")

  private val pipelineStates =
    for (pipeline <- pipelines.toArray) yield {
      pipeline.createState(executionState, queryContext, queryState, resources)
    }

  def pipelineState(pipelineId: PipelineId): PipelineState = pipelineStates(pipelineId.x)

  def reverseIterator: Iterator[PipelineState] = pipelineStates.reverseIterator
}
