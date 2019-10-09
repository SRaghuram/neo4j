/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.mockito.Mockito._
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.runtime.morsel.MockHelper._
import org.neo4j.cypher.internal.runtime.morsel.PipelineState
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class LazyQuerySchedulingTest extends CypherFunSuite {

  test("should order linear execution graph") {
    // given
    val input = Array(
      pipelineState(PipelineId(0)),
      pipelineState(PipelineId(1), PipelineId(0)),
      pipelineState(PipelineId(2), PipelineId(1))
    )

    // when
    val policy = new LazyQueryScheduling(executingQuery(input))

    // then
    policy.pipelineStates.toList should be(List(
      input(2),
      input(1),
      input(0)
    ))
  }

  test("should order branching execution graph") {
    // given
    val input = Array(
      pipelineState(PipelineId(0)),
      pipelineState(PipelineId(1)),
      pipelineState(PipelineId(2), PipelineId(0), PipelineId(1))
    )

    // when
    val policy = new LazyQueryScheduling(executingQuery(input))

    // then
    policy.pipelineStates.toList should be(List(
      input(2),
      input(0),
      input(1)
    ))
  }

  test("should order DAGging execution graph") {
    // given
    val input = Array(
      pipelineState(PipelineId(0)),
      pipelineState(PipelineId(1), PipelineId(0)),
      pipelineState(PipelineId(2), PipelineId(0)),
      pipelineState(PipelineId(3), PipelineId(1), PipelineId(2))
    )

    // when
    val policy = new LazyQueryScheduling(executingQuery(input))

    // then
    policy.pipelineStates.toList should be(List(
      input(3),
      input(1),
      input(0),
      input(2)
    ))
  }

  def executingQuery(states: Array[PipelineState]): ExecutingQuery = {
    val result = mock[ExecutingQuery](RETURNS_DEEP_STUBS)
    when(result.executionState.pipelineStates).thenReturn(states)
    result
  }
}
