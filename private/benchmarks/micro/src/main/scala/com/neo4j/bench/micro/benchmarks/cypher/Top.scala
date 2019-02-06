/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util.Collections

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.{DBL, LNG, STR_SML, _}
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans.Ascending
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._
import scala.collection.mutable

@BenchmarkEnabled(true)
class Top extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var Top_runtime: String = _

  @ParamValues(
    allowed = Array(STR_SML, LNG, DBL),
    base = Array(STR_SML, LNG))
  @Param(Array[String]())
  var Top_sort_type: String = _

  @ParamValues(
    allowed = Array("1", "100", "10000"),
    base = Array("1", "10000"))
  @Param(Array[String]())
  var Top_limit: Long = _

  override def description = "Top, e.g., UNWIND {list} AS x RETURN x ORDER BY x LIMIT {limit}"

  val LIST_ITEM_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(Top_sort_type)
    val listType = symbols.CTList(listElementType)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, unwindListParameter)(IdGen)
    val projection = plans.Projection(leaf, Map("value" -> unwindVariable))(IdGen)
    val limitParameter = astParameter("limit", symbols.CTInteger)
    val top = plans.Top(projection, List(Ascending(unwindVariableName)), limitParameter)(IdGen)
    val resultColumns = List("value")
    val produceResults = plans.ProduceResult(top, columns = resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: TopThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(Top_limit.toInt, visitor)
  }
}

@State(Scope.Thread)
class TopThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: Top): Unit = {
    val unwindListValues: java.util.List[_] = listOf(benchmarkState.Top_sort_type, benchmarkState.LIST_ITEM_COUNT)
    Collections.shuffle(unwindListValues)
    val paramsMap = mutable.Map[String, AnyRef](
      "list" -> unwindListValues,
      "limit" -> Long.box(benchmarkState.Top_limit)).asJava
    benchmarkState.params = ValueUtils.asMapValue(paramsMap)
    executionResult = benchmarkState.buildPlan(from(benchmarkState.Top_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
