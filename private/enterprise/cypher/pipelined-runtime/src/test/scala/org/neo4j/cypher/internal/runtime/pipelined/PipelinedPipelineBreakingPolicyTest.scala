/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.CypherInterpretedPipesFallbackOption.disabled
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PipelinedPipelineBreakingPolicyTest  extends CypherFunSuite {

  test("should fuse together scan and expand in serial mode") {
    // given
    val plan = given(_.expand("(x)-->(y)").allNodeScan("x"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"))

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe false
  }

  test("should not fuse together scan and expand in parallel mode") {
    // given
    val plan = given(_.expand("(x)-->(y)").allNodeScan("x"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = false, readOnly = true, parallelExecution = true),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = true, runtimeName = "Pipelined"))

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe true
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
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"))

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe true
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
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"))

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe false
  }

  test("should refuse to fuse expand over pipeline if RHS of apply not fusable") {
    // given
    val plan = given(_
      .expand("(a)-->(c)")
      .apply()
      .|.sort(Seq(Ascending("a"))) // TODO use .notFusable
      .|.argument("a")
      .allNodeScan("a")
    )

    // when
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"))

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe true
  }

  private def given(subBuilder: LogicalPlanBuilder => LogicalPlanBuilder)= {
    subBuilder( new LogicalPlanBuilder().produceResults("y")).build().lhs.getOrElse(fail())
  }
}
