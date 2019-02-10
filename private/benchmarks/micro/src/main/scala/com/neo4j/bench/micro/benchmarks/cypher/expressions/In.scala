/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import java.util.SplittableRandom

import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher._
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.Values.{intValue, longValue, stringValue}
import org.neo4j.values.virtual.{ListValue, MapValueBuilder, VirtualValues}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class In extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var In_engine: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false"))
  @Param(Array[String]())
  var In_isConstant: Boolean = _

  @ParamValues(
    allowed = Array("10", "100", "1000"),
    base = Array("10", "1000"))
  @Param(Array[String]())
  var In_size: Int = _

  @ParamValues(
    allowed = Array("1", "50", "99"),
    base = Array("1", "50", "99"))
  @Param(Array[String]())
  var In_hitRatio: Int = _

  override def description = "UNWIND $list RETURN $x in [0,1,2,...]"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()


  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
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
    (produceResult, SemanticTable(), resultColumns)
  }

  private def listExpression =
    if (In_isConstant) {
      astListLiteral((0 until In_size).map(i => astLiteralFor(i, LNG)))
    }
    else {
      //this is just to trick the expression to not be fully known at compile time
      astListLiteral((1 to In_size).map(i => astLiteralFor(i, LNG)) :+ astParameter("foo", symbols.CTAny))
    }

  private def nextValueToCheck(rnd: SplittableRandom) =
    longValue(rnd.nextInt((In_size * ( 100.0 / In_hitRatio)).intValue()))

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: InThreadState, bh: Blackhole, rngState: RNGState): Long = {
    val visitor = new CountVisitor(bh)
    threadState.paramBuilder.add("x", nextValueToCheck(rngState.rng))
    threadState.executablePlan.execute(params = threadState.paramBuilder.build(), tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(In.ROWS, visitor)
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
    val useCompiledExpressions = benchmarkState.In_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    paramBuilder.add("list", In.VALUES)
    paramBuilder.add("foo", stringValue("FOO"))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
