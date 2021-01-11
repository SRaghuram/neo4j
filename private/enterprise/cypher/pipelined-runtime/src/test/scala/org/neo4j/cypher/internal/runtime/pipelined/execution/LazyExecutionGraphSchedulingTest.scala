/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.BufferVariant
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.NoSchedulingHint
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition.InterpretedHead
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.physicalplanning.RegularBufferVariant
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LazyExecutionGraphSchedulingTest extends CypherFunSuite {
  private val idGen = new SequentialIdGen()

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
    policy.pipelinesInLazyOrder.toList should be(List(
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
    policy.pipelinesInLazyOrder.toList should be(List(
      PipelineId(2),
      PipelineId(1),
      PipelineId(0),
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
    policy.pipelinesInLazyOrder.toList should be(List(
      PipelineId(3),
      PipelineId(2),
      PipelineId(0),
      PipelineId(1)
    ))
  }

  def pipeline(id: PipelineId,
               lhs: PipelineId = NO_PIPELINE,
               rhs: PipelineId = NO_PIPELINE,
               inputBufferVariant: BufferVariant = RegularBufferVariant,
               headPlan: LogicalPlan = plan): PipelineDefinition = {
    val inputBuffer = BufferDefinition(BufferId(-1), Id.INVALID_ID, ReadOnlyArray.empty, ReadOnlyArray.empty, inputBufferVariant)(null)
    PipelineDefinition(id, lhs, rhs, InterpretedHead(headPlan), inputBuffer, null, null, serial = false, None, NoSchedulingHint)
  }

  def executingGraphDefinition(pipelines: Array[PipelineDefinition]): ExecutionGraphDefinition =
    ExecutionGraphDefinition(null, null, null, pipelines, null)

  def plan:LogicalPlan = Argument()(idGen)
}
