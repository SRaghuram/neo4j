/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.CypherInterpretedPipesFallbackOption.disabled
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipelineBreakingPolicy
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PipelinedPipelineBreakingPolicyTest  extends CypherFunSuite {

  test("should fuse together scan and expand in serial mode") {
    // given
    val plan = given(_.expand("(x)-->(y)").allNodeScan("x"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = 8, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe false
  }

  test("should not fuse together scan and expand in parallel mode") {
    // given
    val plan = given(_.expand("(x)-->(y)").allNodeScan("x"))

    // when
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = false, fusionOverPipelineLimit = 8, readOnly = true, parallelExecution = true),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = true, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

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
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = 8, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

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
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = 8, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe false
  }

  test("should refuse to fuse expand over pipeline if RHS of apply not fusable") {
    // given
    val plan = given(_
      .expand("(a)-->(c)")
      .apply()
      .|.nonFuseable()
      .|.argument("a")
      .allNodeScan("a")
    )

    // when
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = 8, readOnly = true, parallelExecution = false),
      InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe true
  }

  test("should break on expand at fusion over pipeline limit") {
    // given
    val plan = given(_
                       .expand("(a)-->(b)")
                       .filter("a.prop < 10")
                       .expand("(a)-->(b)")
                       .filter("a.prop < 10")
                       .expand("(a)-->(b)")
                       .allNodeScan("a")
                     )

    // positive case
    {
      // when
      val fusionOverPipelineLimit = 3
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe true
    }

    // negative case
    {
      // when
      val fusionOverPipelineLimit = 4
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe false
    }
  }

  test("should break on expand when fusion over pipeline limit is zero") {
    // given
    val plan = given(_
                       .expand("(a)-->(b)")
                       .filter("a.prop < 10")
                       .allNodeScan("a")
                     )

    // when
    val fusionOverPipelineLimit = 0
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                 InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe true
  }

  test("should not break on filter when fusion over pipeline limit is zero") {
    // given
    val plan = given(_
                       .filter("a.prop < 10")
                       .allNodeScan("a")
                     )

    // when
    val fusionOverPipelineLimit = 0
    val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                 InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

    //then
    policy.breakOn(plan, Id.INVALID_ID) shouldBe false
  }

  test("should break on expand at fusion over pipeline limit, with apply") {
    // given
    val plan = given(_
                       .expand("(a)-->(b)")
                       .apply()
                       .|.filter("a.prop < 10")
                       .|.expand("(a)-->(b)")
                       .|.filter("a.prop < 10")
                       .|.expand("(a)-->(b)")
                       .|.argument("a")
                       .allNodeScan("a")
                     )

    // positive case
    {
      // when
      val fusionOverPipelineLimit = 3
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe true
    }

    // negative case
    {
      // when
      val fusionOverPipelineLimit = 4
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe false
    }
  }

  test("should break on expand at fusion over pipeline limit, with non-fusable two-children plan") {
    // given
    val plan = given(_
                       .expand("(a)-->(b)")
                       .expand("(a)-->(b)")
                       .filter("a.prop < 10")
                       .expand("(a)-->(b)")
                       .nodeHashJoin("a") // Not fusable over pipelines
                       .|.filter("b.prop < 10")
                       .|.expand("(a)-->(b)")
                       .|.allNodeScan("a")
                       .allNodeScan("a")
                     )

    // positive case
    {
      // when
      val fusionOverPipelineLimit = 2
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe true
    }

    // negative case
    {
      // when
      val fusionOverPipelineLimit = 3
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe false
    }
  }

  test("should break on expand at fusion over pipeline limit, with non-fusable and apply") {
    // given
    val plan = given(_
                       .expand("(a)-->(b)")
                       .apply()
                       .|.filter("a.prop < 10")
                       .|.expand("(a)-->(b)")
                       .|.filter("a.prop < 10")
                       .|.nonFuseable()
                       .|.expand("(a)-->(b)")
                       .|.argument("a")
                       .allNodeScan("a")
                     )

    // positive case
    {
      // when
      val fusionOverPipelineLimit = 1
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe true
    }

    // negative case
    {
      // when
      val fusionOverPipelineLimit = 2
      val policy = PipelinedPipelineBreakingPolicy(TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true, fusionOverPipelineLimit = fusionOverPipelineLimit, readOnly = true, parallelExecution = false),
                                                   InterpretedPipesFallbackPolicy(disabled, parallelExecution = false, runtimeName = "Pipelined"), parallelExecution = false, SlottedPipelineBreakingPolicy)

      //then
      policy.breakOn(plan, Id.INVALID_ID) shouldBe false
    }
  }

  private def given(subBuilder: LogicalPlanBuilder => LogicalPlanBuilder)= {
    subBuilder( new LogicalPlanBuilder().produceResults("y")).build().lhs.getOrElse(fail())
  }
}
