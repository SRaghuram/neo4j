/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
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
class RollUpApply extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Morsel.NAME),
    base = Array(Slotted.NAME, Morsel.NAME))
  @Param(Array[String]())
  var runtime: String = _

  // No point in parametrizing lhsRows.
  val lhsRows: Int = 1000

  @ParamValues(
    allowed = Array("0", "1", "1000"),
    base = Array("1", "1000"))
  @Param(Array[String]())
  var rhsRows: Int = _

  override def description = "Roll Up Apply"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(lhsRows)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val lhs = "lhs"
    val rhs = "rhs"
    val list = "list"
    val lhsAllNodesScan = plans.AllNodesScan(lhs, Set.empty)(IdGen)
    val rhsAllNodesScan = plans.AllNodesScan(rhs, Set(lhs))(IdGen)
    val limit = plans.Limit(rhsAllNodesScan, astLiteralFor(rhsRows, LNG), DoNotIncludeTies)(IdGen)
    val rollUpApply = plans.RollUpApply(lhsAllNodesScan, limit, list, rhs, Set.empty)(IdGen)
    val resultColumns = List(lhs, list)
    val produceResults = plans.ProduceResult(rollUpApply, columns = resultColumns)(IdGen)

    val table = SemanticTable()
      .addNode(astVariable(lhs))
      .addNode(astVariable(rhs))
      .addVariable(astVariable(list))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: RollUpApplyThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(lhsRows, subscriber)
  }
}

@State(Scope.Thread)
class RollUpApplyThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: RollUpApply): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object RollUpApply {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[RollUpApply])
  }
}
