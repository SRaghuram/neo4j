/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans.{IdGen,  astVariable}
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class AllNodesScan extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var AllNodesScan_runtime: String = _

  override def description = "All Nodes Scan"

  val NODE_COUNT = 1000000
  val EXPECTED_ROW_COUNT = NODE_COUNT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = "node"
    val allNodesScan = plans.AllNodesScan(node, Set.empty)(IdGen)
    val resultColumns = List(node)
    val produceResults = plans.ProduceResult(allNodesScan, columns = resultColumns)(IdGen)

    val table = SemanticTable().addNode(astVariable(node))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AllNodesScanThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class AllNodesScanThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: AllNodesScan): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.AllNodesScan_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
