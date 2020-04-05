/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import java.lang

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphVisualizer.getExecutionPlan
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.DefaultValueMapper

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

//noinspection ZeroIndexToHead
class ExecutionGraphVisualizerTest extends CypherFunSuite {

  private val mapper = new DefaultValueMapper(null)

  case class TNode(props: Map[String, AnyRef], labels: String*)
  case class TRel(startNode: TNode, endNode: TNode, props: Map[String, AnyRef], typ: String)

  test("should return graph for all node scan") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (1: Integer)), "Operator"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
    )

    assertPlanReturns(plan, ops, Seq.empty, bufs, pipes, rels)
  }

  test("should return graph for two linear pipelines with a reduce") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .sort(Seq(Ascending("n"))).withBreak()
      .expand("(n)--(m)").withBreak()
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "Sort", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "Expand", "id" -> (2: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (3: Integer)), "Operator"),
    )
    val asms = Seq(
      TNode(Map("name" -> "ASM[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "Buffer[2]", "id" -> (2: Integer)), "Buffer"),
      TNode(Map("name" -> "ArgumentStateBuffer[3]", "id" -> (3: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[1]", "id" -> (1: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[2]", "id" -> (2: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(bufs(0), asms(0), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(1), asms(0), Map.empty, "HAS_REDUCER"),
      TRel(bufs(2), asms(0), Map.empty, "HAS_REDUCER"),
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(3), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(3), bufs(2), Map.empty, "WRITES_TO"),
      TRel(bufs(2), pipes(1), Map.empty, "READ_BY"),
      TRel(pipes(1), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), bufs(3), Map.empty, "WRITES_TO"),
      TRel(ops(1), asms(0), Map.empty, "USES_ASM"),
      TRel(bufs(3), asms(0), Map.empty, "USES_ASM"),
      TRel(bufs(3), pipes(2), Map.empty, "READ_BY"),
      TRel(pipes(2), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
    )

    assertPlanReturns(plan, ops, asms, bufs, pipes, rels)
  }

  test("should return graph for optional") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .optional().withBreak()
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "Optional", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (2: Integer)), "Operator"),
    )
    val asms = Seq(
      TNode(Map("name" -> "ASM[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "ArgumentStreamBuffer[2]", "id" -> (2: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[1]", "id" -> (1: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(bufs(0), asms(0), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(1), asms(0), Map.empty, "HAS_REDUCER"),
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), bufs(2), Map("argumentSlotOffset" -> (-1: Integer)), "WRITES_TO"),
      TRel(bufs(2), pipes(1), Map.empty, "READ_BY"),
      TRel(pipes(1), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), asms(0), Map.empty, "USES_ASM"),
      TRel(bufs(2), asms(0), Map.empty, "USES_ASM"),
    )

    assertPlanReturns(plan, ops, asms, bufs, pipes, rels)
  }

  test("should return graph for anti") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .anti().withBreak()
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "Anti", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (2: Integer)), "Operator"),
    )
    val asms = Seq(
      TNode(Map("name" -> "ASM[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "AntiBuffer[2]", "id" -> (2: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[1]", "id" -> (1: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(bufs(0), asms(0), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(1), asms(0), Map.empty, "HAS_REDUCER"),
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), bufs(2), Map("argumentSlotOffset" -> (-1: Integer)), "WRITES_TO"),
      TRel(bufs(2), pipes(1), Map.empty, "READ_BY"),
      TRel(pipes(1), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), asms(0), Map.empty, "USES_ASM"),
      TRel(bufs(2), asms(0), Map.empty, "USES_ASM"),
    )

    assertPlanReturns(plan, ops, asms, bufs, pipes, rels)
  }

  test("should return graph for fused single pipeline") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder(fuseAllFactory)
      .produceResults("n")
      .filter("true")
      .expand("(n)--(m)")
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "Selection", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "Expand", "id" -> (2: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (3: Integer)), "Operator"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(3), Map("fused" -> (true: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(3), ops(2), Map("fused" -> (true: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), ops(1), Map("fused" -> (true: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (true: lang.Boolean)), "NEXT_OPERATOR")
    )

    assertPlanReturns(plan, ops, Seq.empty, bufs, pipes, rels)
  }

  test("should return graph for limit and distinct") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .distinct("n AS n")
      .limit(5)
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "Distinct", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "Limit", "id" -> (2: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (3: Integer)), "Operator"),
    )
    val asms = Seq(
      TNode(Map("name" -> "ASM[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
      TNode(Map("name" -> "ASM[1]", "id" -> (1: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(ops(2), asms(0), Map.empty, "USES_ASM"),
      TRel(ops(1), asms(1), Map.empty, "USES_ASM"),
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(bufs(0), asms(1), Map.empty, "HAS_DOWNSTREAM_STATE"),
      TRel(bufs(0), asms(0), Map.empty, "HAS_CANCELLER"),
      TRel(bufs(1), asms(0), Map.empty, "HAS_CANCELLER"),
      TRel(pipes(0), ops(3), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(3), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
    )

    assertPlanReturns(plan, ops, asms, bufs, pipes, rels)
  }

  test("should return graph for hash join") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .nodeHashJoin("n").withBreak()
      .|.nodeByLabelScan("n", "N").withBreak()
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "NodeHashJoin", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "NodeByLabelScan", "id" -> (2: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (3: Integer)), "Operator"),
    )
    val asms = Seq(
      TNode(Map("name" -> "ASM[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
      TNode(Map("name" -> "ASM[1]", "id" -> (1: Integer), "argumentSlotOffset" -> (-1: Integer)), "ASM"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "Buffer[2]", "id" -> (2: Integer)), "Buffer"),
      TNode(Map("name" -> "LHSAccBuffer[3]", "id" -> (3: Integer)), "Buffer"),
      TNode(Map("name" -> "RHSStrBuffer[4]", "id" -> (4: Integer)), "Buffer"),
      TNode(Map("name" -> "MrBuff[5]", "id" -> (5: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[1]", "id" -> (1: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[2]", "id" -> (2: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(ops(1), asms(0), Map.empty, "USES_ASM"),
      TRel(ops(1), asms(1), Map.empty, "USES_ASM"),
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(0), bufs(2), Map.empty, "DELEGATES_TO"),
      TRel(bufs(0), asms(0), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(0), asms(1), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(1), asms(0), Map.empty, "HAS_REDUCER"),
      TRel(bufs(2), asms(1), Map.empty, "HAS_REDUCER"),
      TRel(bufs(3), bufs(5), Map.empty, "DELEGATES_TO"),
      TRel(bufs(4), bufs(5), Map.empty, "DELEGATES_TO"),
      TRel(bufs(3), asms(0), Map.empty, "USES_ASM"),
      TRel(bufs(4), asms(1), Map.empty, "USES_ASM"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(3), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(3), bufs(3), Map("argumentSlotOffset" -> (-1: Integer)), "WRITES_TO"),
      TRel(bufs(2), pipes(1), Map.empty, "READ_BY"),
      TRel(pipes(1), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), bufs(4), Map("argumentSlotOffset" -> (-1: Integer)), "WRITES_TO"),
      TRel(bufs(5), pipes(2), Map.empty, "READ_BY"),
      TRel(pipes(2), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
    )

    assertPlanReturns(plan, ops, asms, bufs, pipes, rels)
  }

  test("should return graph for cartesian product") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .cartesianProduct().withBreak()
      .|.nodeByLabelScan("m", "M").withBreak()
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      TNode(Map("name" -> "CartesianProduct", "id" -> (1: Integer)), "Operator"),
      TNode(Map("name" -> "NodeByLabelScan", "id" -> (2: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (3: Integer)), "Operator"),
    )
    val asms = Seq(
      TNode(Map("name" -> "ASM[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (0: Integer)), "ASM"),
      TNode(Map("name" -> "ASM[1]", "id" -> (1: Integer), "argumentSlotOffset" -> (0: Integer)), "ASM"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "AttachBuffer[2]", "id" -> (2: Integer), "argumentSlotOffset" -> (-1: Integer)), "Buffer"),
      TNode(Map("name" -> "ApplyBuffer[3]", "id" -> (3: Integer), "argumentSlotOffset" -> (0: Integer)), "Buffer"),
      TNode(Map("name" -> "Buffer[4]", "id" -> (4: Integer)), "Buffer"),
      TNode(Map("name" -> "LHSAccBuffer[5]", "id" -> (5: Integer)), "Buffer"),
      TNode(Map("name" -> "RHSStrBuffer[6]", "id" -> (6: Integer)), "Buffer"),
      TNode(Map("name" -> "MrBuff[7]", "id" -> (7: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[1]", "id" -> (1: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[2]", "id" -> (2: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(ops(1), asms(0), Map.empty, "USES_ASM"),
      TRel(ops(1), asms(1), Map.empty, "USES_ASM"),
      TRel(bufs(5), asms(0), Map.empty, "USES_ASM"),
      TRel(bufs(6), asms(1), Map.empty, "USES_ASM"),
      TRel(bufs(3), asms(0), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(3), asms(1), Map.empty, "HAS_REDUCER_ON_RHS"),
      TRel(bufs(4), asms(1), Map.empty, "HAS_REDUCER"),

      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(3), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(3), bufs(2), Map.empty, "WRITES_TO"),
      TRel(bufs(2), bufs(3), Map.empty, "DELEGATES_TO"),
      // I would have expected such an edge to be here: (b3)-[:DELEGATES_TO]->(b5), but the communication happens implicitly through (b3)-[:HAS_REDUCER_ON_RHS]->(asm0)<-[:USES_ASM]-(b5)
      TRel(bufs(5), bufs(7), Map.empty, "DELEGATES_TO"),

      TRel(bufs(3), bufs(4), Map.empty, "DELEGATES_TO"),
      TRel(bufs(4), pipes(1), Map.empty, "READ_BY"),
      TRel(pipes(1), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), bufs(6), Map("argumentSlotOffset" -> (0: Integer)), "WRITES_TO"),
      TRel(bufs(6), bufs(7), Map.empty, "DELEGATES_TO"),

      TRel(bufs(7), pipes(2), Map.empty, "READ_BY"),
      TRel(pipes(2), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
    )
    assertPlanReturns(plan, ops, asms, bufs, pipes, rels)
  }

  test("should return graph for apply") {
    val plan = getExecutionPlan(new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .apply().withBreak()
      .|.nodeByLabelScan("n", "N", "n").withBreak()
      .allNodeScan("n").withBreak()
      .build())

    val ops = Seq(
      TNode(Map("name" -> "ProduceResult", "id" -> (0: Integer)), "End", "Operator"),
      // No node for Apply, since it appears nowhere in the execution graph. Note the ops-index/id mismatch in the test because of that.
      TNode(Map("name" -> "NodeByLabelScan", "id" -> (2: Integer)), "Operator"),
      TNode(Map("name" -> "AllNodesScan", "id" -> (3: Integer)), "Operator"),
    )
    val bufs = Seq(
      TNode(Map("name" -> "ApplyBuffer[0]", "id" -> (0: Integer), "argumentSlotOffset" -> (-1: Integer)), "Start", "Buffer"),
      TNode(Map("name" -> "Buffer[1]", "id" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "ApplyBuffer[2]", "id" -> (2: Integer), "argumentSlotOffset" -> (1: Integer)), "Buffer"),
      TNode(Map("name" -> "Buffer[3]", "id" -> (3: Integer)), "Buffer"),
    )
    val pipes = Seq(
      TNode(Map("name" -> "Pipeline[0]", "id" -> (0: Integer), "serial" -> (false: lang.Boolean)), "Pipeline"),
      TNode(Map("name" -> "Pipeline[1]", "id" -> (1: Integer), "serial" -> (true: lang.Boolean)), "Pipeline"),
    )
    val rels = Set(
      TRel(bufs(0), bufs(1), Map.empty, "DELEGATES_TO"),
      TRel(bufs(1), pipes(0), Map.empty, "READ_BY"),
      TRel(pipes(0), ops(2), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(2), bufs(2), Map.empty, "WRITES_TO"),
      TRel(bufs(2), bufs(3), Map.empty, "DELEGATES_TO"),
      TRel(bufs(3), pipes(1), Map.empty, "READ_BY"),
      TRel(pipes(1), ops(1), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
      TRel(ops(1), ops(0), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR"),
    )

    assertPlanReturns(plan, ops, Seq.empty, bufs, pipes, rels)
  }

  private def assertPlanReturns(plan: ExecutionPlan,
                                expectedOperators: Seq[TNode],
                                expectedASMs: Seq[TNode],
                                expectedBuffers: Seq[TNode],
                                expectedPipelines: Seq[TNode],
                                expectedRels: Set[TRel]): Unit = {
    val expectedNodes = (expectedOperators ++ expectedASMs ++ expectedBuffers ++ expectedPipelines).toSet
    val subscriber = TestSubscriber.singleThreaded

    // when
    val runtimeResult = plan.run(null, null, null, prePopulateResults = false, null, subscriber)
    runtimeResult.request(1)
    runtimeResult.await()

    val actualNodes = subscriber.lastSeen(0)
      .map(mapper)
      .asInstanceOf[java.util.List[Node]]
      .asScala
      .map(asTNode)
      .toSet

    val actualRels = subscriber.lastSeen(1)
      .map(mapper)
      .asInstanceOf[java.util.List[Relationship]]
      .asScala
      .map(asTRel)
      .toSet

    // then
    actualNodes should equal(expectedNodes)
    actualRels should equal(expectedRels)
  }

  private def asTNode(node: Node): TNode = TNode(node.getAllProperties.asScala.toMap, node.getLabels.asScala.toSeq.map(_.name()): _*)
  private def asTRel(rel: Relationship): TRel = TRel(asTNode(rel.getStartNode), asTNode(rel.getEndNode), rel.getAllProperties.asScala.toMap, rel.getType.name())

  val fuseAllFactory: OperatorFuserFactory =
    (headPlanId: Id, inputSlotConfiguration: SlotConfiguration) =>
      new OperatorFuser {
        val plans = new ArrayBuffer[LogicalPlan]

        override def fuseIn(plan: LogicalPlan): Boolean = {
          plans += plan
          true
        }

        override def fuseIn(output: OutputDefinition): Boolean =
          output match {
            case ProduceResultOutput(p) => fuseIn(p)
            case ReduceOutput(_, p) => fuseIn(p)
            case _ => false
          }

        override def fusedPlans: IndexedSeq[LogicalPlan] = plans
      }
}
