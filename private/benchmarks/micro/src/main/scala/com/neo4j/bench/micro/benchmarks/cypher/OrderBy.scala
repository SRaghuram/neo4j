/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util.Collections

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans.Ascending
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class OrderBy extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var OrderBy_runtime: String = _

  /*
  Compiled runtime does not support Order By of Temporal/Spatial types
   */
  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var OrderBy_type: String = _

  override def description = "Order By, e.g., UNWIND {list} AS x RETURN x ORDER BY x"

  val EXPECTED_ROW_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(OrderBy_type)

    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, parameter)(IdGen)
    val orderBy = plans.Sort(leaf, List(Ascending(unwindVariableName)))(IdGen)
    val resultColumns = List(unwindVariableName)
    val produceResults = plans.ProduceResult(orderBy, columns = resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: OrderByThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class OrderByThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: OrderBy): Unit = {
    val list = listOf(benchmarkState.OrderBy_type, benchmarkState.EXPECTED_ROW_COUNT)
    Collections.shuffle(list)
    benchmarkState.params = mapValuesOfList("list", list)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.OrderBy_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
