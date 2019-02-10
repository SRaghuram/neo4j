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
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder, TypeParamValues}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.virtual.{ListValue, MapValue, VirtualValues}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class MapProjection extends AbstractCypherBenchmark {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var MapProjection_engine: String = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("1", "10", "100"))
  @Param(Array[String]())
  var MapProjection_size: Int = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false"))
  @Param(Array[String]())
  var MapProjection_includeAllProps: Boolean = _

  override def description = "UNWIND $list WITH map as $map RETURN map{.*, k1: 'updated1', k2: 'updated2',...} AS result"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val resultColumns = List("result")
    val mapProjections = (0 until MapProjection_size).map(i => s"k$i" -> astLiteralFor(s"updated%i", STR_SML))
    val expression = astMapProjection("map", MapProjection_includeAllProps, mapProjections)
    val listType = symbols.CTList(symbols.CTAny)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, unwindListParameter)(IdGen)
    val projection1 = plans.Projection(leaf, Map("map" -> astParameter("map", symbols.CTAny)))(IdGen)
    val projection = plans.Projection(projection1, Map("result" -> expression))(IdGen)
    val produceResult = plans.ProduceResult(projection, columns = resultColumns)(IdGen)
    (produceResult, SemanticTable(), resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: MapProjectionThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(params = threadState.params, tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(MapProjection.ROWS, visitor)
  }
}

object MapProjection {

  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(Values.intValue).toArray: _*)
  val MAP: MapValue = VirtualValues.map((1 to 100).map(i => s"k$i").toArray,
                                        (1 to 100).map(i => Values.stringValue(s"v$i")).toArray)
  def main(args: Array[String]): Unit = {
    Main.run(classOf[MapProjection], args: _*)
  }
}

@State(Scope.Thread)
class MapProjectionThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _

  @Setup
  def setUp(benchmarkState: MapProjection, rngState: RNGState): Unit = {
    val useCompiledExpressions = benchmarkState.MapProjection_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
    params = VirtualValues.map(Array("map", "list"),
                               Array(
                                 MapProjection.MAP,
                                 MapProjection.VALUES))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}