/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans.{IdGen, astVariable}
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class CartesianProduct extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME),
    base = Array(Slotted.NAME))
  @Param(Array[String]())
  var CartesianProduct_runtime: String = _

  override def description = "Cartesian Product"

  private val NODE_COUNT = 1000
  private val EXPECTED_ROW_COUNT = NODE_COUNT * NODE_COUNT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val a = "a"
    val b = "b"
    val allNodesScanA = plans.AllNodesScan(a, Set.empty)(IdGen)
    val allNodesScanB = plans.AllNodesScan(b, Set.empty)(IdGen)
    val cartesianProduct = plans.CartesianProduct(allNodesScanA, allNodesScanB)(IdGen)
    val resultColumns = List(a, b)
    val produceResults = plans.ProduceResult(cartesianProduct, columns = resultColumns)(IdGen)

    val table = SemanticTable().addNode(astVariable(a)).addNode(astVariable(b))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: CartesianProductThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class CartesianProductThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: CartesianProduct): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.CartesianProduct_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
