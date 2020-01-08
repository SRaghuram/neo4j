/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util
import java.util.Collections

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class PartialSort extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Morsel.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  /*
  Compiled runtime does not support Order By of Temporal/Spatial types
   */
  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML),
    base = Array(LNG))
  @Param(Array[String]())
  var propertyType: String = _

  @ParamValues(
    allowed = Array("1", "10", "100", "1000", "10000", "100000", "1000000"),
    base = Array("1000"))
  @Param(Array[Int]())
  var distinctCount = 1000

  @ParamValues(
    allowed = Array("PartialSort", "FullSort"),
    base = Array("PartialSort"))
  @Param(Array[String]())
  var sortMode = "PartialSort"

  override def description = "PartialSort, e.g., UNWIND {listOfMapValuesSortedByA} AS tuples RETURN tuples ORDER BY tuples.a, tuples.b"

  val EXPECTED_ROW_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(propertyType)

    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val aVariableName = "valueA"
    val bVariableName = "valueB"
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, parameter)(IdGen)
    val projection = plans.Projection(leaf, Map(aVariableName -> astProperty(unwindVariable, "a"), bVariableName -> astProperty(unwindVariable, "b")))(IdGen)
    val sort = sortMode match {
      case "PartialSort" => plans.PartialSort(projection, List(Ascending(aVariableName)), List(Ascending(bVariableName)))(IdGen)
      case "FullSort" => plans.Sort(projection, List(Ascending(aVariableName), Ascending(bVariableName)))(IdGen)
    }


    val resultColumns = List(aVariableName, bVariableName)
    val produceResults = plans.ProduceResult(sort, columns = resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: PartialSortThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class PartialSortThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: PartialSort): Unit = {
    // A list with a certain number of distinct values
    val listA = randomListOf[java.lang.Comparable[Any]](benchmarkState.propertyType, benchmarkState.EXPECTED_ROW_COUNT, benchmarkState.distinctCount)
    // Sort this list
    listA.sort(util.Comparator.naturalOrder())

    // A list of ascending values
    val listB = listOf(benchmarkState.propertyType, benchmarkState.EXPECTED_ROW_COUNT)
    // Randomize the order
    Collections.shuffle(listB)

    // Zip the lists together in MapValues
    var i = 0
    val list = new util.ArrayList[util.Map[String, Any]]
    while (i < listA.size()) {

      val map = new util.HashMap[String, Any]()
      map.put("a", listA.get(i))
      map.put("b", listB.get(i))
      list.add(map)
      i += 1
    }

    benchmarkState.params = mapValuesOfList("list", list)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
