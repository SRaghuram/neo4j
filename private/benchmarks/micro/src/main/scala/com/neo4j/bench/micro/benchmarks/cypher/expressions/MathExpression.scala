/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import java.lang.Math.PI

import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher._
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.virtual.{ListValue, MapValue, VirtualValues}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class MathExpression extends AbstractCypherBenchmark {

@ParamValues(
  allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
  base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
@Param(Array[String]())
  var MathExpression_engine: String = _

  override def description = "UNWIND $list RETURN rand() * ( sin($x) * sin($x) + cos($x) * cos($x) ) AS result"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val resultColumns = List("result")
    val parameter = Parameter("x", symbols.CTAny)(Pos)
    val expression = astMultiply(astFunctionInvocation("rand"),
      astAdd(
        astMultiply(
          astFunctionInvocation("sin", parameter),
          astFunctionInvocation("sin", parameter)
        ),
        astMultiply(
          astFunctionInvocation("cos", parameter),
          astFunctionInvocation("cos", parameter)
        )))

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
  def executePlan(threadState: MathExpressionThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(params = threadState.params, tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(MathExpression.ROWS, visitor)
  }
}

object MathExpression {
  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(Values.intValue).toArray:_*)
  def main(args: Array[String]): Unit = {
    Main.run(classOf[MathExpression], args:_*)
  }
}

@State(Scope.Thread)
class MathExpressionThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _

  @Setup
  def setUp(benchmarkState: MathExpression, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.MathExpression_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    params = VirtualValues.map(Array("x", "list"),
                               Array(
                                 doubleValue(rngState.rng.nextDouble(2 * PI)),
                                 MathExpression.VALUES))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

