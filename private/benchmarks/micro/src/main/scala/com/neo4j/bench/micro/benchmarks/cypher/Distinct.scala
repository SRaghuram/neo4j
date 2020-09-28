/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astProperty
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
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
class Distinct extends AbstractCypherBenchmark {
  private final val ONE_COLUMN_PRIMITIVE = "primitive"
  private final val ONE_COLUMN_LNG = LNG
  private final val ONE_COLUMN_STR_SML = STR_SML
  private final val TWO_COLUMNS_PRIMITIVE = "primitive-primitive"
  private final val TWO_COLUMNS_PRIMITIVE_LNG = "primitive-" + LNG
  private final val TWO_COLUMNS_PRIMITIVE_STR_SML = "primitive-" + STR_SML

  private val NODE_A = "nodeA"
  private val NODE_B = "nodeB"
  private val PROPERTY = "prop"

  private lazy val NODE_COUNT = columns match {
    case ONE_COLUMN_PRIMITIVE | ONE_COLUMN_LNG | ONE_COLUMN_STR_SML => 10000
    case TWO_COLUMNS_PRIMITIVE | TWO_COLUMNS_PRIMITIVE_LNG | TWO_COLUMNS_PRIMITIVE_STR_SML => 100
  }

  private val TOLERATED_ROW_COUNT_ERROR = 0.10
  private val EXPECTED_ROW_COUNT = 10000
  private val EXPECTED_ROW_COUNT_MIN = Math.round(EXPECTED_ROW_COUNT - EXPECTED_ROW_COUNT * TOLERATED_ROW_COUNT_ERROR).toInt
  private val EXPECTED_ROW_COUNT_MAX = Math.round(EXPECTED_ROW_COUNT + EXPECTED_ROW_COUNT * TOLERATED_ROW_COUNT_ERROR).toInt

  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array(ONE_COLUMN_PRIMITIVE, ONE_COLUMN_LNG, ONE_COLUMN_STR_SML, TWO_COLUMNS_PRIMITIVE, TWO_COLUMNS_PRIMITIVE_LNG, TWO_COLUMNS_PRIMITIVE_STR_SML),
    base = Array(ONE_COLUMN_PRIMITIVE, TWO_COLUMNS_PRIMITIVE, TWO_COLUMNS_PRIMITIVE_STR_SML))
  @Param(Array[String]())
  var columns: String = _

  override def description = "Distinct only, e.g., MATCH (n), (m) RETURN DISTINCT n, m." + PROPERTY

  override protected def getConfig: DataGeneratorConfig = {
    val builder = new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
    withProperty(columns, builder)
      .build()
  }

  def withProperty(columnDefinition: String, builder: DataGeneratorConfigBuilder): DataGeneratorConfigBuilder = columns match {
    case ONE_COLUMN_PRIMITIVE => builder
    case ONE_COLUMN_LNG | ONE_COLUMN_STR_SML => builder.withNodeProperties(randPropertyFor(columnDefinition, PROPERTY))
    case TWO_COLUMNS_PRIMITIVE => builder
    case TWO_COLUMNS_PRIMITIVE_LNG | TWO_COLUMNS_PRIMITIVE_STR_SML => builder.withNodeProperties(randPropertyFor(columnDefinition.split("-")(1), PROPERTY))
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val table = SemanticTable()
      .addNode(astVariable(NODE_A))
      .addNode(astVariable(NODE_B))
    columns match {
      case ONE_COLUMN_PRIMITIVE =>
        val allNodesScanA = plans.AllNodesScan(NODE_A, Set.empty)(IdGen)
        val allNodesScanB = plans.AllNodesScan(NODE_B, Set.empty)(IdGen)
        val cartesianProduct = plans.CartesianProduct(allNodesScanA, allNodesScanB)(IdGen)
        val distinct = plans.Distinct(cartesianProduct, Map(NODE_B -> astVariable(NODE_B)))(IdGen)
        val resultColumns = List(NODE_B)
        val produceResults = plans.ProduceResult(distinct, columns = resultColumns)(IdGen)
        TestSetup(produceResults, table, resultColumns)

      case ONE_COLUMN_LNG | ONE_COLUMN_STR_SML =>
        val allNodesScanA = plans.AllNodesScan(NODE_A, Set.empty)(IdGen)
        val nodePropertyA = astProperty(astVariable(NODE_A), PROPERTY)
        val projectionA = plans.Projection(
          allNodesScanA,
          Map(PROPERTY -> nodePropertyA))(IdGen)
        val allNodesScanB = plans.AllNodesScan(NODE_B, Set.empty)(IdGen)
        val cartesianProduct = plans.CartesianProduct(projectionA, allNodesScanB)(IdGen)
        val distinct = plans.Distinct(
          cartesianProduct,
          Map(PROPERTY -> nodePropertyA))(IdGen)
        val resultColumns = List(PROPERTY)
        val produceResults = plans.ProduceResult(distinct, columns = resultColumns)(IdGen)
        TestSetup(produceResults, table, resultColumns)

      case TWO_COLUMNS_PRIMITIVE =>
        val allNodesScanA = plans.AllNodesScan(NODE_A, Set.empty)(IdGen)
        val allNodesScanB = plans.AllNodesScan(NODE_B, Set.empty)(IdGen)
        val cartesianProduct = plans.CartesianProduct(allNodesScanA, allNodesScanB)(IdGen)
        val distinct = plans.Distinct(
          cartesianProduct,
          Map(NODE_A -> astVariable(NODE_A), NODE_B -> astVariable(NODE_B)))(IdGen)
        val resultColumns = List(NODE_A, NODE_B)
        val produceResults = plans.ProduceResult(distinct, columns = resultColumns)(IdGen)
        TestSetup(produceResults, table, resultColumns)

      case TWO_COLUMNS_PRIMITIVE_LNG | TWO_COLUMNS_PRIMITIVE_STR_SML =>
        val allNodesScanA = plans.AllNodesScan(NODE_A, Set.empty)(IdGen)
        val nodePropertyA = astProperty(astVariable(NODE_A), PROPERTY)
        val projectionA = plans.Projection(
          allNodesScanA,
          Map(PROPERTY -> nodePropertyA))(IdGen)
        val allNodesScanB = plans.AllNodesScan(NODE_B, Set.empty)(IdGen)
        val cartesianProduct = plans.CartesianProduct(projectionA, allNodesScanB)(IdGen)
        val distinct = plans.Distinct(
          cartesianProduct,
          Map(PROPERTY -> nodePropertyA, NODE_B -> astVariable(NODE_B)))(IdGen)
        val resultColumns = List(PROPERTY, NODE_B)
        val produceResults = plans.ProduceResult(distinct, columns = resultColumns)(IdGen)
        TestSetup(produceResults, table, resultColumns)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: DistinctThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT_MIN, EXPECTED_ROW_COUNT_MAX, subscriber)
  }
}

@State(Scope.Thread)
class DistinctThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Distinct): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
