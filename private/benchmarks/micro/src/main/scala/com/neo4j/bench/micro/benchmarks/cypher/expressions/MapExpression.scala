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
class MapExpression extends AbstractCypherBenchmark {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var MapExpression_engine: String = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("10"))
  @Param(Array[String]())
  var MapExpression_size: Int = _

  override def description = "UNWIND $list RETURN {k1: $x, k2: $x, ..., k_size: $x) AS result"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val resultColumns = List("result")
    val parameter = Parameter("x", symbols.CTAny)(Pos)
    val mapExpressions = (0 until MapExpression_size).map(i => s"k$i" -> parameter)
    val expression = astMap(mapExpressions: _*)
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
  def executePlan(threadState: MapExpressionThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(params = threadState.params, tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(MapExpression.ROWS, visitor)
  }
}

object MapExpression {

  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(Values.intValue).toArray: _*)

  def main(args: Array[String]): Unit = {
    Main.run(classOf[MapExpression], args: _*)
  }
}

@State(Scope.Thread)
class MapExpressionThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _

  @Setup
  def setUp(benchmarkState: MapExpression, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.MapExpression_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    params = VirtualValues.map(Array("x", "list"),
                               Array(
                                 doubleValue(rngState.rng.nextDouble()),
                                 MapExpression.VALUES))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}





