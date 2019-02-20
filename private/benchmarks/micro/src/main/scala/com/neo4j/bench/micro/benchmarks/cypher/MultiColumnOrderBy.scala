/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.{LNG, STR_SML, mapValuesOfList}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols.ListType
import org.neo4j.cypher.internal.frontend.v3_3.{ExpressionTypeInfo, SemanticTable, symbols}
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.v3_3.logical.plans.{Ascending, ColumnOrder, Descending}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._
import scala.util.Random

@BenchmarkEnabled(true)
class MultiColumnOrderBy extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var MultiColumnOrderBy_runtime: String = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var MultiColumnOrderBy_type: String = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("1", "100"))
  @Param(Array[String]())
  var MultiColumnOrderBy_columns: Int = _

  override def description = "Order By multiple columns, e.g., UNWIND [{a:4},{a:2},...] AS x RETURN x.a, x.b, ... ORDER BY x.a, x.b, ..."

  private val EXPECTED_ROW_COUNT = 100000
  var params: MapValue = _

  def values: java.util.List[java.util.Map[String, Any]] = {
    val rng = new Random(42)
    val rngRange = 10
    MultiColumnOrderBy_type match {
      case STR_SML =>
        List.range(0, EXPECTED_ROW_COUNT).map(i =>
          columnNames().map(keyName => keyName -> rng.nextInt(rngRange).toString).toMap[String, Any].asJava
        ).asJava
      case LNG =>
        List.range(0, EXPECTED_ROW_COUNT).map(i =>
          columnNames().map(keyName => keyName -> rng.nextInt(rngRange).toLong).toMap[String, Any].asJava
        ).asJava
      case _ => throw new IllegalArgumentException(s"Unsupported type: $MultiColumnOrderBy_type")
    }
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = toInnerType(MultiColumnOrderBy_type)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.SingleRow()(Solved), unwindVariable.name, parameter)(Solved)
    val expressionsAndSortItems: Map[String, (Expression, ColumnOrder)] = columnNames()
      .map(keyName => "c" + keyName -> (astProperty(unwindVariable, keyName), sortItemFor(keyName)))
      .toMap[String, (Expression, ColumnOrder)]
    val expressions = expressionsAndSortItems.map {
      case (key: String, value: (Expression, ColumnOrder)) => key -> value._1
    }
    val projection = plans.Projection(leaf, expressions)(Solved)
    // TODO randomize Ascending/Descending?
    val sortItems: Seq[ColumnOrder] = expressionsAndSortItems.keys.map(key => Ascending(key)).toSeq
    val orderBy = plans.Sort(projection, sortItems)(Solved)
    val resultColumns = expressionsAndSortItems.keys.toList
    val produceResults = plans.ProduceResult(resultColumns, orderBy)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  private def toInnerType(columnType: String): ListType = symbols.CTList(cypherTypeFor(columnType))

  private def columnNames(): List[String] = List.range(0, MultiColumnOrderBy_columns).map(i => keyNameFor(i))

  private def keyNameFor(i: Int) = i.toString

  private def sortItemFor(key: String) =
    if (Integer.parseInt(key) % 2 == 0) Ascending(key) else Descending(key)

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: MultiColumnOrderByThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class MultiColumnOrderByThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: MultiColumnOrderBy): Unit = {
    benchmarkState.params = mapValuesOfList("list", benchmarkState.values)
    executionResult = benchmarkState.buildPlan(from(benchmarkState.MultiColumnOrderBy_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
