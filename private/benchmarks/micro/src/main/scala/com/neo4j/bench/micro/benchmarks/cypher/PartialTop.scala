/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util
import java.util.Collections

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.{DBL, LNG, STR_SML, _}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans.Ascending
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._
import scala.collection.mutable

@BenchmarkEnabled(true)
class PartialTop extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var PartialTop_runtime: String = _

  /*
  Compiled runtime does not support Order By of Temporal/Spatial types
   */
  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML),
    base = Array(LNG))
  @Param(Array[String]())
  var PartialTop_type: String = _

  @ParamValues(
    allowed = Array("1", "100", "10000"),
    base = Array("1", "10000"))
  @Param(Array[String]())
  var PartialTop_limit: Long = _

  @ParamValues(
    allowed = Array("1", "10", "100", "1000", "10000", "100000", "1000000"),
    base = Array("1000"))
  @Param(Array[Int]())
  var PartialTop_distinctCount = 1000

  @ParamValues(
    allowed = Array("PartialTop", "NormalTop"),
    base = Array("PartialTop"))
  @Param(Array[String]())
  var PartialTop_sortMode = "PartialTop"

  override def description = "PartialTop, e.g., UNWIND {listOfMapValuesSortedByA} AS tuples RETURN tuples ORDER BY tuples.a, tuples.b LIMIT {limit}"

  val LIST_ITEM_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(PartialTop_type)

    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val aVariableName = "valueA"
    val bVariableName = "valueB"
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, parameter)(IdGen)
    val projection = plans.Projection(leaf, Map(aVariableName -> astProperty(unwindVariable, "a"), bVariableName -> astProperty(unwindVariable, "b")))(IdGen)
    val limitParameter = astParameter("limit", symbols.CTInteger)
    val top = PartialTop_sortMode match {
      case "PartialTop" => plans.PartialTop(projection, List(Ascending(aVariableName)), List(Ascending(bVariableName)), limitParameter)(IdGen)
      case "NormalTop" => plans.Top(projection, List(Ascending(unwindVariableName)), limitParameter)(IdGen)
    }


    val resultColumns = List(aVariableName, bVariableName)
    val produceResults = plans.ProduceResult(top, columns = resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: PartialTopThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(PartialTop_limit.toInt, visitor)
  }
}

@State(Scope.Thread)
class PartialTopThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: PartialTop): Unit = {
    // A list with a certain number of distinct values
    val listA = randomListOf[java.lang.Comparable[Any]](benchmarkState.PartialTop_type, benchmarkState.LIST_ITEM_COUNT, benchmarkState.PartialTop_distinctCount)
    // Sort this list
    listA.sort(util.Comparator.naturalOrder())

    // A list of ascending values
    val listB = listOf(benchmarkState.PartialTop_type, benchmarkState.LIST_ITEM_COUNT)
    // Randomize the order
    Collections.shuffle(listB)

    // Zip the lists together in MapValues
    var i = 0
    val list = new util.ArrayList[util.Map[String, Any]]
    while(i < listA.size()) {

      val map = new util.HashMap[String, Any]()
      map.put("a", listA.get(i))
      map.put("b", listB.get(i))
      list.add(map)
      i += 1
    }

    val paramsMap = mutable.Map[String, AnyRef](
      "list" -> list,
      "limit" -> Long.box(benchmarkState.PartialTop_limit)).asJava
    benchmarkState.params = ValueUtils.asMapValue(paramsMap)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.PartialTop_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
