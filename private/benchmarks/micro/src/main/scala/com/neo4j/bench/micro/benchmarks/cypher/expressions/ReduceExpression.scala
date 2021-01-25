/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.AbstractCypherBenchmark
import com.neo4j.bench.micro.benchmarks.cypher.ExecutablePlan
import com.neo4j.bench.micro.benchmarks.cypher.Slotted
import com.neo4j.bench.micro.benchmarks.cypher.TestSetup
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.Pos
import com.neo4j.bench.micro.data.Plans.astAdd
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astReduce
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
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
class ReduceExpression extends AbstractCypherBenchmark {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var engine: String = _

  @ParamValues(
    allowed = Array("10", "100", "1000"),
    base = Array("100"))
  @Param(Array[String]())
  var size: Int = _

  override def description = "UNWIND $list RETURN reduce(sum = 0, n in $x | sum + n) AS result"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val resultColumns = List("result")
    val parameter = Parameter("x", symbols.CTAny)(Pos)
    val expression = astReduce("sum", astLiteralFor(0L, LNG), "n", parameter, astAdd(astVariable("sum"), astVariable("n")))
    val listType = symbols.CTList(symbols.CTAny)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, unwindListParameter)(IdGen)
    val projection = plans.Projection(leaf, Map("result" -> expression))(IdGen)
    val produceResult = plans.ProduceResult(projection, columns = resultColumns)(IdGen)
    TestSetup(produceResult, SemanticTable(), resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ReduceExpressionThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params = threadState.params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(ReduceExpression.ROWS, subscriber)
  }
}

object ReduceExpression {

  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(Values.intValue).toArray: _*)

  def main(args: Array[String]): Unit = {
    Main.run(classOf[ReduceExpression], args: _*)
  }
}

@State(Scope.Thread)
class ReduceExpressionThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _
  var list: ListValue = _

  @Setup
  def setUp(benchmarkState: ReduceExpression, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(Slotted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    list = VirtualValues.list((1 to benchmarkState.size).map(Values.intValue).toArray: _*)
    params = VirtualValues.map(Array("x", "list"),
      Array(list, ReduceExpression.VALUES))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}










