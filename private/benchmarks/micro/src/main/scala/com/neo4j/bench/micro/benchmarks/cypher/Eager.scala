/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans.{astLiteralFor, _}
import com.neo4j.bench.micro.data.TypeParamValues.{LNG, STR_SML}
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans.{DoNotIncludeTies, LogicalPlan}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class Eager extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var Eager_runtime: String = _

  @ParamValues(
    allowed = Array("0", "4"),
    base = Array("4"))
  @Param(Array[String]())
  var Eager_referenceColumns: Int = _

  @ParamValues(
    allowed = Array("1", "4"),
    base = Array("1", "4"))
  @Param(Array[String]())
  var Eager_primitiveColumns: Int = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(STR_SML))
  @Param(Array[String]())
  var Eager_refType: String = _

  override def description = "MATCH (n1), (n2) WITH n1, n2, 1 AS r1, 2 AS r2 LIMIT count RETURN n1, n2, r1, r2"

  val NODE_COUNT = 1000000
  val EXPECTED_ROW_COUNT = NODE_COUNT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val nodeName = s"n$Eager_primitiveColumns"
    val leftAllNodesScan = plans.AllNodesScan(nodeName, Set.empty)(IdGen)
    val (cartesianProducts, table, nodeNames) = buildCartesianProducts(
      Eager_primitiveColumns - 1,
      leftAllNodesScan,
      SemanticTable().addNode(astVariable(nodeName)),
      List(nodeName))
    val limit = plans.Limit(cartesianProducts, astLiteralFor(NODE_COUNT, LNG), DoNotIncludeTies)(IdGen)
    val projectColumns = Range(0, Eager_referenceColumns).map(i => (s"r$i", astLiteralFor(i, Eager_refType))).toMap
    val projection = plans.Projection(limit, projectColumns)(IdGen)
    val eager = plans.Eager(projection)(IdGen)
    val resultColumns = nodeNames ++ projectColumns.keys.toList
    val produceResult = plans.ProduceResult(eager, columns = resultColumns)(IdGen)
    (produceResult, table, resultColumns)
  }

  private def buildCartesianProducts(count: Int, left: LogicalPlan, table: SemanticTable, nodeNames: List[String]): (plans.LogicalPlan, SemanticTable, List[String]) = {
    if (count == 0)
      (left, table, nodeNames)
    else {
      val nodeName = s"n$count"
      val allNodesScan = plans.AllNodesScan(nodeName, Set.empty)(IdGen)
      buildCartesianProducts(
        count - 1,
        plans.CartesianProduct(left, allNodesScan)(IdGen),
        table.addNode(astVariable(nodeName)),
        nodeNames :+ nodeName)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: EagerThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class EagerThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: Eager): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.Eager_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
