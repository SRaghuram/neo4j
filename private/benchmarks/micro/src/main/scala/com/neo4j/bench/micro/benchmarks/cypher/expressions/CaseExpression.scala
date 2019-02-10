/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher._
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.{LNG, STR_SML}
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.virtual.{ListValue, MapValueBuilder, VirtualValues}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class CaseExpression extends AbstractCypherBenchmark {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var CaseExpression_engine: String = _

  @ParamValues(
    allowed = Array("2", "4", "8", "16"),
    base = Array("8"))
  @Param(Array[String]())
  var CaseExpression_size: Int = _

  override def description = "UNWIND $list RETURN CASE $p WHEN 1 THEN '1' WHEN 2 THEN '2'... ELSE 'DEFAULT'"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val resultColumns = List("result")
    val parameter = Parameter("x", symbols.CTAny)(Pos)
    val alternatives = (0 until CaseExpression_size).map(i =>  astLiteralFor(i, LNG) -> astLiteralFor(i.toString, STR_SML))

    val expression = astCase(parameter, alternatives, Some( astLiteralFor("DEFAULT", STR_SML)))
    val listType = symbols.CTList(symbols.CTAny)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, unwindListParameter)(IdGen)
    val projection = plans.Projection(leaf, Map("result" -> expression))(IdGen)
    val produceResult = plans.ProduceResult(projection, columns = resultColumns)(IdGen)
    (produceResult, SemanticTable(), resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: CaseExpressionThreadState, bh: Blackhole, rngState: RNGState): Long = {
    val visitor = new CountVisitor(bh)
    threadState.paramBuilder.add("x", doubleValue(rngState.rng.nextInt(CaseExpression_size + 1)))
    val result = threadState.executablePlan.execute(params = threadState.paramBuilder.build(), tx = threadState.tx)
    result.accept(visitor)
    assertExpectedRowCount(CaseExpression.ROWS, visitor)
  }
}

object CaseExpression {

  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(Values.intValue).toArray: _*)

  def main(args: Array[String]): Unit = {
    Main.run(classOf[CaseExpression], args: _*)
  }
}

@State(Scope.Thread)
class CaseExpressionThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  val paramBuilder = new MapValueBuilder(2)

  @Setup
  def setUp(benchmarkState: CaseExpression, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.CaseExpression_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()

    paramBuilder.add("list", CaseExpression.VALUES)
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
