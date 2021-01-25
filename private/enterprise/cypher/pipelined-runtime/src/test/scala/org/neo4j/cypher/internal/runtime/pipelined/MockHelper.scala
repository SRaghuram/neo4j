/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OutputOperator
import org.scalatest.mockito.MockitoSugar

object MockHelper extends MockitoSugar {
  /**
   * Get a Mock of PipelineState
   * @param id the Id of the pipeline
   * @param lhs the Id of the LHS of the Pipeline
   * @param rhs the Id of the RHS of the Pipeline
   * @param schedulingResults a sequence of answers to calls to `nextTask`
   * @return
   */
  def pipelineState(id: PipelineId,
                    lhs: PipelineId = NO_PIPELINE,
                    rhs: PipelineId = NO_PIPELINE,
                    schedulingResults: Seq[InvocationOnMock => SchedulingResult[PipelineTask]] = Seq.empty): PipelineState = {
    val result = mock[PipelineState]
    val pipeline = ExecutablePipeline(
      id,
      lhs,
      rhs,
      mock[Operator](RETURNS_DEEP_STUBS),
      Array(),
      serial = false,
      null,
      null,
      mock[OutputOperator](RETURNS_DEEP_STUBS),
      None,
    )
    when(result.pipeline).thenReturn(pipeline)
    if(schedulingResults.nonEmpty) {
      schedulingResults.foldLeft(when(result.nextTask(any[PipelinedQueryState], any[QueryResources]))) { (ongoingMock, sr) =>
        ongoingMock.thenAnswer(sr(_))
      }
    }
    result
  }
}
