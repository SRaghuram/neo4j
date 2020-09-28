/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.ProcedureHelpers.TestProcedures
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.TypeParamValues
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
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

@BenchmarkEnabled(true)
class StandAloneProcedure extends AbstractProcedureCall {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  override def description = "CALL bench.procedure(value)"

  @ParamValues(
    allowed = Array("1000", "1000000"),
    base = Array("1000"))
  @Param(Array[String]())
  var outputRowsPerInputRow: Int = _

  override protected def procedureName(procedures: GlobalProcedures): plans.QualifiedName = {
    procedures.registerProcedure(classOf[TestProcedures])
    new plans.QualifiedName(Array[String]("bench"), "procedure")
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val rowCount = astLiteralFor(outputRowsPerInputRow, TypeParamValues.LNG)
    val procedureCall = plans.ProcedureCall(plans.Argument()(IdGen), resolvedCall(Seq(rowCount)))(IdGen)
    val resultColumns = List("value")
    val produceResults = plans.ProduceResult(procedureCall, resultColumns)(IdGen)
    val table = SemanticTable()
    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: StandAloneProcedureThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(outputRowsPerInputRow, subscriber)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlanFetchOne(threadState: StandAloneProcedureThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    while (result.await()) {
      result.request(1)
    }
    assertExpectedRowCount(outputRowsPerInputRow, subscriber)
  }
}

object StandAloneProcedure {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[StandAloneProcedure])
  }
}

@State(Scope.Thread)
class StandAloneProcedureThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: StandAloneProcedure): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
