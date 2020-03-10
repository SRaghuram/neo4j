/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LazyExecutionGraphSchedulingTest extends CypherFunSuite {

  test("should order linear execution graph") {
    // given
    val input = Array(
      pipeline(PipelineId(0)),
      pipeline(PipelineId(1), lhs = PipelineId(0)),
      pipeline(PipelineId(2), lhs = PipelineId(1))
    )

    // when
    val policy = new LazyExecutionGraphScheduling(executingGraphDefinition(input))

    // then
    policy.pipelinesInLHSDepthFirstOrder.toList should be(List(
      PipelineId(2),
      PipelineId(1),
      PipelineId(0)
    ))
  }

  test("should order branching execution graph") {
    // given
    val input = Array(
      pipeline(PipelineId(0)),
      pipeline(PipelineId(1)),
      pipeline(PipelineId(2), lhs = PipelineId(0), rhs = PipelineId(1))
    )

    // when
    val policy = new LazyExecutionGraphScheduling(executingGraphDefinition(input))

    // then
    policy.pipelinesInLHSDepthFirstOrder.toList should be(List(
      PipelineId(2),
      PipelineId(0),
      PipelineId(1)
    ))
  }

  test("should order DAGging execution graph") {
    // given
    val input = Array(
      pipeline(PipelineId(0)),
      pipeline(PipelineId(1), lhs = PipelineId(0)),
      pipeline(PipelineId(2), lhs = PipelineId(0)),
      pipeline(PipelineId(3), lhs = PipelineId(1), rhs = PipelineId(2))
    )

    // when
    val policy = new LazyExecutionGraphScheduling(executingGraphDefinition(input))

    // then
    policy.pipelinesInLHSDepthFirstOrder.toList should be(List(
      PipelineId(3),
      PipelineId(1),
      PipelineId(0),
      PipelineId(2)
    ))
  }

  def pipeline(id: PipelineId,
               lhs: PipelineId = NO_PIPELINE,
               rhs: PipelineId = NO_PIPELINE): PipelineDefinition =
    PipelineDefinition(id, lhs, rhs, null, null, null, null, serial = false)

  def executingGraphDefinition(pipelines: Array[PipelineDefinition]): ExecutionGraphDefinition =
    ExecutionGraphDefinition(null, null, null, pipelines, null)
}
