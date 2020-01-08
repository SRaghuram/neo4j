/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class StandAloneProcedureCall extends AbstractProcedureCall {

  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1", "100", "1000"),
    base = Array("1", "100", "1000"))
  @Param(Array[String]())
  var labels: Int = _

  override def description = "Stand-alone procedure call, CALL db.labels"

  override protected def getConfig: DataGeneratorConfig = {
    val configuredLabels = (0 until labels).map(c => Label.label(s"label$c"))
    new DataGeneratorConfigBuilder()
      .withNodeCount(1)
      .withLabels(configuredLabels:_*)
      .isReusableStore(true)
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val call = plans.ProcedureCall(plans.Argument()(IdGen), resolvedCall)(IdGen)
    (plans.ProduceResult(call, columns)(IdGen),semanticTable, columns.toList)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: StandAloneProcedureCallThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(labels, subscriber)
  }

  override protected def procedureName: QualifiedName = QualifiedName(Seq("db"), "labels")
}

object StandAloneProcedureCall {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[StandAloneProcedureCall], args:_*)
  }
}

@State(Scope.Thread)
class StandAloneProcedureCallThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: StandAloneProcedureCall): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
