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
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.graphdb.Label
import org.neo4j.kernel.api.procedure.GlobalProcedures
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

@BenchmarkEnabled(false)
class StandAloneProcedureCall extends AbstractProcedureCall {

  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
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

  override def setup(planContext: PlanContext): TestSetup = {
    val call = plans.ProcedureCall(plans.Argument()(IdGen), resolvedCall(Seq.empty))(IdGen)
    TestSetup(plans.ProduceResult(call, columns)(IdGen), semanticTable, columns.toList)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: StandAloneProcedureCallThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(labels, subscriber)
  }

  override protected def procedureName(procedures: GlobalProcedures): QualifiedName = QualifiedName(Seq("db"), "labels")
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
