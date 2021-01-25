/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astFunctionInvocation
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.Plans.cypherTypeFor
import com.neo4j.bench.micro.data.TypeParamValues.DBL
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.TypeParamValues.mapValuesOfList
import com.neo4j.bench.micro.data.TypeParamValues.shuffledListOf
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.logical.plans
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
class GroupingAndAggregation extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1", "100", "10000"),
    base = Array("1", "100", "10000"))
  @Param(Array[Int]())
  var distinctCount: Int = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML),
    base = Array(STR_SML))
  @Param(Array[String]())
  var propertyType: String = _

  override def description = "Grouping & Aggregation, e.g., MATCH (n) RETURN n, count(n)"

  val VALUE_COUNT = 1000000

  var params: MapValue = _

  override def setup(planContext: PlanContext): TestSetup = {
    val listElementType = cypherTypeFor(propertyType)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwind = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariable.name, parameter)(IdGen)
    val groupingExpressions = Map("value" -> unwindVariable)
    val aggregationExpressions = Map("count" -> astFunctionInvocation(Count.name, unwindVariable))
    val aggregation = plans.Aggregation(unwind, groupingExpressions, aggregationExpressions)(IdGen)
    val resultColumns = List("value", "count")
    val produceResults = plans.ProduceResult(aggregation, columns = resultColumns)(IdGen)
    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))
    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: GroupingAndAggregationThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(distinctCount, subscriber)
  }
}

@State(Scope.Thread)
class GroupingAndAggregationThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: GroupingAndAggregation): Unit = {
    benchmarkState.params = mapValuesOfList(
      "list",
      shuffledListOf(benchmarkState.propertyType, benchmarkState.VALUE_COUNT, benchmarkState.distinctCount))
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
