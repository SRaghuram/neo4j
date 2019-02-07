/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher._
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class FilterExpression extends AbstractCypherBenchmark with ListExpressionsHelper {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var FilterExpression_engine: String = _

  @ParamValues(
    allowed = Array("10", "100", "1000"),
    base = Array("1000"))
  @Param(Array[String]())
  var FilterExpression_size: Int = _

  override def description = "UNWIND 10000_element_list AS no_used RETURN filter(n in $x WHERE n > <literal>) AS result"

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listParameter = astParameter("x", symbols.CTAny)
    val listExpression = astFilter("n", listParameter, astGt(astVariable("n"), astLiteralFor(midPoint, LNG)))
    listExpressionPlan(planContext, listParameter, listExpression)
  }

  def midPoint: Int = FilterExpression_size / 2

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: FilterExpressionThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    val result = threadState.executablePlan.execute(params = threadState.params, tx = threadState.tx)
    result.accept(visitor)
    assertExpectedRowCount(ROWS, visitor)
  }
}

@State(Scope.Thread)
class FilterExpressionThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _

  @Setup
  def setUp(benchmarkState: FilterExpression, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.FilterExpression_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    params = benchmarkState.getParams(benchmarkState.FilterExpression_size)
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
