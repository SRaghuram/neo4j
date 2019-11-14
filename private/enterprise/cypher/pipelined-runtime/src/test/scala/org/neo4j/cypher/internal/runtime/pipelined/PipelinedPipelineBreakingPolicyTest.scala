/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.CypherInterpretedPipesFallbackOption.disabled
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PipelinedPipelineBreakingPolicyTest  extends CypherFunSuite {

  test("should fuse together scan and expand in serial mode") {
    // given
    val plan = given(_.expand("(x)-->(y)").allNodeScan("x"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(OperatorFusionPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true),
                                                 InterpretedPipesFallbackPolicy(disabled, parallelExecution = false))

    //then
    policy.breakOn(plan) shouldBe false
  }

  test("should not fuse together scan and expand in parallel mode") {
    // given
    val plan = given(_.expand("(x)-->(y)").allNodeScan("x"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(OperatorFusionPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = false),
                                                 InterpretedPipesFallbackPolicy(disabled, parallelExecution = true))

    //then
    policy.breakOn(plan) shouldBe true
  }

  test("should not fuse expand over pipeline if head operator in pipeline is non-fusable") {
    // given
    val plan = given(_
                       .expand("(a)-->(c)")
                       .filter("true")
                       .aggregation(Seq("a AS a"), Seq("collect(b) AS bs"))
                       .expand("(a)-[r1]->(b)")
                       .allNodeScan("a"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(OperatorFusionPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true),
                                                 InterpretedPipesFallbackPolicy(disabled, parallelExecution = false))

    //then
    policy.breakOn(plan) shouldBe true
  }

  test("should fuse expand over pipeline if head operator in pipeline is fusable") {
    // given
    val plan = given(_
                       .expand("(c)-->(b)")
                       .expand("(a)-->(c)")
                       .filter("true")
                       .aggregation(Seq("a AS a"), Seq("collect(b) AS bs"))
                       .expand("(a)-[r1]->(b)")
                       .allNodeScan("a"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(OperatorFusionPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true),
                                                 InterpretedPipesFallbackPolicy(disabled, parallelExecution = false))

    //then
    policy.breakOn(plan) shouldBe false
  }

  private def given(subBuilder: LogicalPlanBuilder => LogicalPlanBuilder)= {
    subBuilder( new LogicalPlanBuilder().produceResults("y")).build().lhs.getOrElse(fail())
  }
}
