/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.AntiType
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.ArgumentStreamBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStreamType
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.BufferVariant
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.physicalplanning.RegularBufferVariant
import org.neo4j.cypher.internal.util.attribution.Id
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

  test("should order DAGing execution graph") {
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

  test("should prioritize scheduling pipelines taking work from a reducing buffer") {
    // given
    val input = Array(
      pipeline(PipelineId(0)),
      pipeline(PipelineId(1), lhs = PipelineId(0)),
      pipeline(PipelineId(2), lhs = PipelineId(1), inputBufferVariant = ArgumentStateBufferVariant(ArgumentStateMapId(0))),
      pipeline(PipelineId(3), lhs = PipelineId(2)),
      pipeline(PipelineId(4), lhs = PipelineId(3)),
      pipeline(PipelineId(5), lhs = PipelineId(4), inputBufferVariant = ArgumentStateBufferVariant(ArgumentStateMapId(1))),
      pipeline(PipelineId(6), lhs = PipelineId(5))
    )

    // when
    val policy = new LazyExecutionGraphScheduling(executingGraphDefinition(input))

    // then
    policy.priority.toList should be(List(
      false, // pipeline 6
      true,
      false,
      false,
      true,
      false,
      true  // pipeline 0
    ))
  }

  test("should prioritize scheduling pipelines taking work from an argument stream buffer") {
    // given
    val input = Array(
      pipeline(PipelineId(0)),
      pipeline(PipelineId(1), lhs = PipelineId(0)),
      pipeline(PipelineId(2), lhs = PipelineId(1), inputBufferVariant = ArgumentStreamBufferVariant(ArgumentStateMapId(0), AntiType)),
      pipeline(PipelineId(3), lhs = PipelineId(2), inputBufferVariant = ArgumentStreamBufferVariant(ArgumentStateMapId(1), ArgumentStreamType)),
      pipeline(PipelineId(4), lhs = PipelineId(3))
    )

    // when
    val policy = new LazyExecutionGraphScheduling(executingGraphDefinition(input))

    // then
    policy.priority.toList should be(List(
      false, // pipeline 4
      true,
      true,
      false,
      true  // pipeline 0
    ))
  }

  test("should prioritize left-most leaf") {
    // given
    val input = Array(
      pipeline(PipelineId(0)),
      pipeline(PipelineId(1)),
      pipeline(PipelineId(2), lhs = PipelineId(0), rhs = PipelineId(1))
    )

    // when
    val policy = new LazyExecutionGraphScheduling(executingGraphDefinition(input))

    // then
    policy.priority.toList should be(List(false, true, false))
  }

  def pipeline(id: PipelineId,
               lhs: PipelineId = NO_PIPELINE,
               rhs: PipelineId = NO_PIPELINE,
               inputBufferVariant: BufferVariant = RegularBufferVariant): PipelineDefinition = {
    val inputBuffer = BufferDefinition(BufferId(-1), Id.INVALID_ID, ReadOnlyArray.empty, ReadOnlyArray.empty, inputBufferVariant)(null)
    PipelineDefinition(id, lhs, rhs, null, inputBuffer, null, null, serial = false, None)
  }

  def executingGraphDefinition(pipelines: Array[PipelineDefinition]): ExecutionGraphDefinition =
    ExecutionGraphDefinition(null, null, null, pipelines, null)
}
