/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.AbstractCypherBenchmark
import com.neo4j.bench.micro.benchmarks.cypher.ExecutablePlan
import com.neo4j.bench.micro.benchmarks.cypher.Slotted
import com.neo4j.bench.micro.benchmarks.cypher.TestSetup
import com.neo4j.bench.micro.data.Plans.astAny
import com.neo4j.bench.micro.data.Plans.astGt
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
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
class AnyInListExpression extends AbstractCypherBenchmark with ListExpressionsHelper {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var engine: String = _

  @ParamValues(
    allowed = Array("10", "100", "1000"),
    base = Array("1000"))
  @Param(Array[String]())
  var size: Int = _

  override def description = "UNWIND 10000_element_list AS no_used RETURN any(n in $x WHERE n > <literal>) AS result"

  override def setup(planContext: PlanContext): TestSetup = {
    val listParameter = astParameter("x", symbols.CTAny)
    val listExpression = astAny("n", listParameter, astGt(astVariable("n"), astLiteralFor(midPoint, LNG)))
    listExpressionPlan(planContext, listParameter, listExpression)
  }

  def midPoint: Int = size / 2

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AnyInListExpressionThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params = threadState.params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(ROWS, subscriber)
  }
}

@State(Scope.Thread)
class AnyInListExpressionThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _

  @Setup
  def setUp(benchmarkState: AnyInListExpression, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(Slotted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    params = benchmarkState.getParams(benchmarkState.size)
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
