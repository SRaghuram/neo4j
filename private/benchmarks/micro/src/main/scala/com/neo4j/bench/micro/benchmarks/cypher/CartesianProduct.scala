/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astVariable
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class CartesianProduct extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1", "10", "100", "1000"),
    base = Array("1", "1000"))
  @Param(Array[String]())
  var rows: Int = _

  override def description = "Cartesian Product"

  private var expectedRowCount: Int = _

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(rows)
      .isReusableStore(true)
      .build()

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    expectedRowCount = rows * rows
  }

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
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(expectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class CartesianProductThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: CartesianProduct): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
