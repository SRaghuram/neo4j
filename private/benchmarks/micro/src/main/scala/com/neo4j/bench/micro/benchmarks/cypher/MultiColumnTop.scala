/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.{LNG, STR_SML}
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans.{Ascending, ColumnOrder, Descending}
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.cypher.internal.v4_0.util.symbols.ListType
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

@BenchmarkEnabled(true)
class MultiColumnTop extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var MultiColumnTop_runtime: String = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(STR_SML))
  @Param(Array[String]())
  var MultiColumnTop_type: String = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("100"))
  @Param(Array[String]())
  var MultiColumnTop_columns: Int = _

  @ParamValues(
    allowed = Array("1", "100", "10000"),
    base = Array("1", "10000"))
  @Param(Array[String]())
  var MultiColumnTop_limit: Long = _

  override def description = "Top with multiple columns, e.g., UNWIND [{a:4},{a:2},...] AS x RETURN x.a, x.b, ... ORDER BY x.a, x.b LIMIT {limit}, ..."

  private val LIST_ITEM_COUNT = 100000
  var params: MapValue = _

  def values: java.util.List[java.util.Map[String, Any]] = {
    val rng = new Random(42)
    val rngRange = 10
    MultiColumnTop_type match {
      case STR_SML =>
        List.range(0, LIST_ITEM_COUNT).map(i =>
          columnNames().map(keyName => keyName -> rng.nextInt(rngRange).toString).toMap[String, Any].asJava
        ).asJava
      case LNG =>
        List.range(0, LIST_ITEM_COUNT).map(i =>
          columnNames().map(keyName => keyName -> rng.nextInt(rngRange).toLong).toMap[String, Any].asJava
        ).asJava
      case _ => throw new IllegalArgumentException(s"Unsupported type: $MultiColumnTop_type")
    }
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = toInnerType(MultiColumnTop_type)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariable.name, parameter)(IdGen)
    val expressionsAndSortItems: Map[String, (Expression, ColumnOrder)] = columnNames()
      .map(keyName => "c" + keyName -> (astProperty(unwindVariable, keyName), sortItemFor(keyName)))
      .toMap[String, (Expression, ColumnOrder)]
    val expressions = expressionsAndSortItems.map {
      case (key: String, value: (Expression, ColumnOrder)) => key -> value._1
    }
    val projection = plans.Projection(leaf, expressions)(IdGen)
    // TODO randomize Ascending/Descending?
    val sortItems: Seq[ColumnOrder] = expressionsAndSortItems.keys.map(key => Ascending(key)).toSeq
    val limitParameter = astParameter("limit", symbols.CTInteger)
    val top = plans.Top(projection, sortItems, limitParameter)(IdGen)
    val resultColumns = expressionsAndSortItems.keys.toList
    val produceResults = plans.ProduceResult(top, resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  private def toInnerType(columnType: String): ListType = symbols.CTList(cypherTypeFor(columnType))

  private def columnNames(): List[String] = List.range(0, MultiColumnTop_columns).map(i => keyNameFor(i))

  private def keyNameFor(i: Int) = i.toString

  private def sortItemFor(key: String) =
    if (Integer.parseInt(key) % 2 == 0) Ascending(key) else Descending(key)

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: MultiColumnTopThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(MultiColumnTop_limit.toInt, visitor)
  }
}

@State(Scope.Thread)
class MultiColumnTopThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: MultiColumnTop): Unit = {
    val paramsMap = mutable.Map[String, AnyRef](
      "list" -> benchmarkState.values,
      "limit" -> Long.box(benchmarkState.MultiColumnTop_limit)).asJava
    benchmarkState.params = ValueUtils.asMapValue(paramsMap)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.MultiColumnTop_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
