/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util.Collections

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.{DBL, LNG, STR_SML, _}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v3_5.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans.Ascending
import org.neo4j.cypher.internal.v3_5.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._
import scala.collection.mutable

@BenchmarkEnabled(true)
class Top extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array(STR_SML, LNG, DBL),
    base = Array(STR_SML, LNG))
  @Param(Array[String]())
  var sort_type: String = _

  @ParamValues(
    allowed = Array("1", "100", "10000"),
    base = Array("1", "10000"))
  @Param(Array[String]())
  var limit: Long = _

  override def description = "Top, e.g., UNWIND $list AS x RETURN x ORDER BY x LIMIT $limit"

  val LIST_ITEM_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(sort_type)
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
    threadState.executablePlan.execute(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(limit.toInt, visitor)
  }
}

@State(Scope.Thread)
class TopThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Top): Unit = {
    val unwindListValues: java.util.List[_] = listOf(benchmarkState.sort_type, benchmarkState.LIST_ITEM_COUNT)
    Collections.shuffle(unwindListValues)
    val paramsMap = mutable.Map[String, AnyRef](
      "list" -> unwindListValues,
      "limit" -> Long.box(benchmarkState.limit)).asJava
    benchmarkState.params = ValueUtils.asMapValue(paramsMap)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
