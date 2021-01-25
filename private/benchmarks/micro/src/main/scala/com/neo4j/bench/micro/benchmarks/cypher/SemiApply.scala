/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.LNG
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
class SemiApply extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1000"),
    base = Array("1000"))
  @Param(Array[String]())
  var lhsRows: Int = _

  @ParamValues(
    allowed = Array("0", "1", "1000"),
    base = Array("0", "1000"))
  @Param(Array[String]())
  var rhsRows: Int = _

  override def description = "Semi Apply"

  private var expectedRowCount: Int = _

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    if (rhsRows > lhsRows) {
      throw new IllegalStateException("In this benchmark RHS row count may not exceed LHS row count")
    }
    expectedRowCount = if (rhsRows > 0) lhsRows else 0
  }

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(lhsRows)
      .isReusableStore(true)
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val lhs = "lhs"
    val rhs = "rhs"
    val lhsAllNodesScan = plans.AllNodesScan(lhs, Set.empty)(IdGen)
    val rhsAllNodesScan = plans.AllNodesScan(rhs, Set(lhs))(IdGen)
    val limit = plans.Limit(rhsAllNodesScan, astLiteralFor(rhsRows, LNG))(IdGen)
    val semiApply = plans.SemiApply(lhsAllNodesScan, limit)(IdGen)
    val resultColumns = List(lhs)
    val produceResults = plans.ProduceResult(semiApply, columns = resultColumns)(IdGen)

    val table = SemanticTable()
      .addNode(astVariable(lhs))
      .addNode(astVariable(rhs))

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: SemiApplyThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(expectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class SemiApplyThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: SemiApply): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object SemiApply {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[SemiApply])
  }
}
