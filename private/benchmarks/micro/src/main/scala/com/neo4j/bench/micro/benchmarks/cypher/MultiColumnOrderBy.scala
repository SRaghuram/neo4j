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
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astProperty
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.Plans.cypherTypeFor
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.TypeParamValues.mapValuesOfList
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.ListType
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

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.Random

@BenchmarkEnabled(true)
class MultiColumnOrderBy extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(STR_SML))
  @Param(Array[String]())
  var propertyType: String = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("100"))
  @Param(Array[String]())
  var columns: Int = _

  override def description = "Order By multiple columns, e.g., UNWIND [{a:4},{a:2},...] AS x RETURN x.a, x.b, ... ORDER BY x.a, x.b, ..."

  private val EXPECTED_ROW_COUNT = 100000
  var params: MapValue = _

  def values: java.util.List[java.util.Map[String, Any]] = {
    val rng = new Random(42)
    val rngRange = 10
    propertyType match {
      case STR_SML =>
        List.range(0, EXPECTED_ROW_COUNT).map(i =>
          columnNames().map(keyName => keyName -> rng.nextInt(rngRange).toString).toMap[String, Any].asJava
        ).asJava
      case LNG =>
        List.range(0, EXPECTED_ROW_COUNT).map(i =>
          columnNames().map(keyName => keyName -> rng.nextInt(rngRange).toLong).toMap[String, Any].asJava
        ).asJava
      case _ => throw new IllegalArgumentException(s"Unsupported type: $propertyType")
    }
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val listElementType = toInnerType(propertyType)
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
    val orderBy = plans.Sort(projection, sortItems)(IdGen)
    val resultColumns = expressionsAndSortItems.keys.toList
    val produceResults = plans.ProduceResult(orderBy, resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    TestSetup(produceResults, table, resultColumns)
  }

  private def toInnerType(columnType: String): ListType = symbols.CTList(cypherTypeFor(columnType))

  private def columnNames(): List[String] = List.range(0, columns).map(i => keyNameFor(i))

  private def keyNameFor(i: Int) = i.toString

  private def sortItemFor(key: String) =
    if (Integer.parseInt(key) % 2 == 0) Ascending(key) else Descending(key)

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: MultiColumnOrderByThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class MultiColumnOrderByThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: MultiColumnOrderBy): Unit = {
    benchmarkState.params = mapValuesOfList("list", benchmarkState.values)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
