/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.ProcedureHelpers.TestProcedures
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astFunctionInvocation
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astVariable
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
class ProcedureCallingCypher extends AbstractProcedureCall {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  override def description = "UNWIND range(1,inputRows) AS i CALL bench.cypher(value) YIELD"

  @ParamValues(
    allowed = Array("1000", "1000000"),
    base = Array("1000"))
  @Param(Array[String]())
  var inputRows: Int = _

  @ParamValues(
    allowed = Array("1", "1000", "1000000"),
    base = Array("1", "1000"))
  @Param(Array[String]())
  var outputRowsPerInputRow: Int = _

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(outputRowsPerInputRow)
      .isReusableStore(true)
      .build()

  override protected def procedureName(procedures: GlobalProcedures): plans.QualifiedName = {
    procedures.registerProcedure(classOf[TestProcedures])
    new plans.QualifiedName(Array[String]("bench"), "cypher")
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val query = astLiteralFor("MATCH (n) RETURN n", TypeParamValues.STR_SML)
    val range = astFunctionInvocation("range", astLiteralFor(1, TypeParamValues.LNG), astLiteralFor(inputRows, TypeParamValues.LNG))
    val unwindVariable = astVariable("value")
    val unwind = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariable.name, range)(IdGen)
    val procedureCall = plans.ProcedureCall(unwind, resolvedCall(Seq(query)))(IdGen)
    val resultColumns = List("value")
    val produceResults = plans.ProduceResult(procedureCall, resultColumns)(IdGen)
    val table = SemanticTable()
    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ProcedureCallingCypherThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(inputRows * outputRowsPerInputRow, subscriber)
  }
}

object ProcedureCallingCypher {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[ProcedureCallingCypher], args:_*)
  }
}

@State(Scope.Thread)
class ProcedureCallingCypherThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: ProcedureCallingCypher): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
