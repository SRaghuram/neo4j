/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder

import java.util.SplittableRandom
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.AbstractCypherBenchmark
import com.neo4j.bench.micro.benchmarks.cypher.ExecutablePlan
import com.neo4j.bench.micro.benchmarks.cypher.Slotted
import com.neo4j.bench.micro.benchmarks.cypher.TestSetup
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astIn
import com.neo4j.bench.micro.data.Plans.astListLiteral
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.utf8Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValueBuilder
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
class In extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var engine: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false"))
  @Param(Array[String]())
  var isConstant: Boolean = _

  @ParamValues(
    allowed = Array("10", "100", "1000"),
    base = Array("10", "1000"))
  @Param(Array[String]())
  var size: Int = _

  @ParamValues(
    allowed = Array("1", "50", "99"),
    base = Array("1", "50", "99"))
  @Param(Array[String]())
  var hitRatio: Int = _

  override def description = "UNWIND $list RETURN $x in [0,1,2,...]"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()


  override def setup(planContext: PlanContext): TestSetup = {
    val resultColumns = List("result")
    val parameter = astParameter("x", symbols.CTAny)
    val expression = astIn(parameter, listExpression)

    val listType = symbols.CTList(symbols.CTAny)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, unwindListParameter)(IdGen)
    val projection = plans.Projection(leaf, Map("result" -> expression))(IdGen)
    val produceResult = plans.ProduceResult(projection, columns = resultColumns)(IdGen)
    TestSetup(produceResult, SemanticTable(), resultColumns)
  }

  private def listExpression =
    if (isConstant) {
      astListLiteral((0 until size).map(i => astLiteralFor(i, LNG)))
    }
    else {
      //this is just to trick the expression to not be fully known at compile time
      astListLiteral((1 to size).map(i => astLiteralFor(i, LNG)) :+ astParameter("foo", symbols.CTAny))
    }

  private def nextValueToCheck(rnd: SplittableRandom) =
    longValue(rnd.nextInt((size * ( 100.0 / hitRatio)).intValue()))

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: InThreadState, bh: Blackhole, rngState: RNGState): Long = {
    threadState.paramBuilder.add("x", nextValueToCheck(rngState.rng))
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params = threadState.paramBuilder.build(), tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(In.ROWS, subscriber)
  }
}

object In {
  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(intValue).toArray:_*)
  def main(args: Array[String]): Unit = {
    Main.run(classOf[In], args:_*)
  }
}

@State(Scope.Thread)
class InThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  val paramBuilder = new MapValueBuilder()

  @Setup
  def setUp(benchmarkState: In): Unit = {
    val useCompiledExpressions = benchmarkState.engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(Slotted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    paramBuilder.add("list", In.VALUES)
    paramBuilder.add("foo", utf8Value("FOO"))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
