/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class NodeByLabelScan extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var NodeByLabelScan_runtime: String = _

  override def description = "Node By Label Scan"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val EXPECTED_ROW_COUNT = NODE_COUNT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val nodeByLabelScan = plans.NodeByLabelScan("node", astLabelName(LABEL), Set.empty)(IdGen)
    val resultColumns = List("node")
    val produceResults = plans.ProduceResult(nodeByLabelScan, columns = resultColumns)(IdGen)

    val table = SemanticTable()

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: NodeByLabelScanThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class NodeByLabelScanThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: NodeByLabelScan): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.NodeByLabelScan_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
