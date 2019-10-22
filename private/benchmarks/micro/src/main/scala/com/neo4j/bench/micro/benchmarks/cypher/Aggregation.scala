/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.expressions.functions.Count
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class Aggregation extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME, Parallel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array(STR_SML, LNG, DBL),
    base = Array(STR_SML))
  @Param(Array[String]())
  var propertyType: String = _

  override def description = "Aggregation only, e.g., MATCH (n) RETURN count(n)"

  val VALUE_COUNT = 1000000
  val EXPECTED_ROW_COUNT = 1

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(propertyType)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), "value", parameter)(IdGen)
    val groupingExpressions = Map[String, Expression]()
    val aggregationExpressions = Map("count" -> astFunctionInvocation(Count.name, astVariable("value")))
    val aggregation = plans.Aggregation(leaf, groupingExpressions, aggregationExpressions)(IdGen)
    val resultColumns = List("count")
    val produceResults = plans.ProduceResult(aggregation, resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AggregationThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, threadState.tx, subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class AggregationThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Aggregation): Unit = {
    benchmarkState.params = mapValuesOfList("list", listOf(benchmarkState.propertyType, benchmarkState.VALUE_COUNT))
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
