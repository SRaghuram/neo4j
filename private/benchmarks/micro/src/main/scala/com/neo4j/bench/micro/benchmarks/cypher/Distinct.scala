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
import com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
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

  private lazy val NODE_COUNT = Distinct_columns match {
    case ONE_COLUMN_PRIMITIVE | ONE_COLUMN_LNG | ONE_COLUMN_STR_SML => 10000
    case TWO_COLUMNS_PRIMITIVE | TWO_COLUMNS_PRIMITIVE_LNG | TWO_COLUMNS_PRIMITIVE_STR_SML => 100
  }

  private val TOLERATED_ROW_COUNT_ERROR = 0.10
  private val EXPECTED_ROW_COUNT = 10000
  private val EXPECTED_ROW_COUNT_MIN = Math.round(EXPECTED_ROW_COUNT - EXPECTED_ROW_COUNT * TOLERATED_ROW_COUNT_ERROR).toInt
  private val EXPECTED_ROW_COUNT_MAX = Math.round(EXPECTED_ROW_COUNT + EXPECTED_ROW_COUNT * TOLERATED_ROW_COUNT_ERROR).toInt

  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var Distinct_runtime: String = _

  @ParamValues(
    allowed = Array(ONE_COLUMN_PRIMITIVE, ONE_COLUMN_LNG, ONE_COLUMN_STR_SML, TWO_COLUMNS_PRIMITIVE, TWO_COLUMNS_PRIMITIVE_LNG, TWO_COLUMNS_PRIMITIVE_STR_SML),
    base = Array(ONE_COLUMN_PRIMITIVE, TWO_COLUMNS_PRIMITIVE, TWO_COLUMNS_PRIMITIVE_STR_SML))
  @Param(Array[String]())
  var Distinct_columns: String = _

  override def description = "Distinct only, e.g., MATCH (n), (m) RETURN DISTINCT n, m." + PROPERTY

  override protected def getConfig: DataGeneratorConfig = {
    val builder = new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
    withProperty(Distinct_columns, builder)
      .build()
  }

  def withProperty(columnDefinition: String, builder: DataGeneratorConfigBuilder): DataGeneratorConfigBuilder = Distinct_columns match {
    case ONE_COLUMN_PRIMITIVE => builder
    case ONE_COLUMN_LNG | ONE_COLUMN_STR_SML => builder.withNodeProperties(randPropertyFor(columnDefinition, PROPERTY))
    case TWO_COLUMNS_PRIMITIVE => builder
    case TWO_COLUMNS_PRIMITIVE_LNG | TWO_COLUMNS_PRIMITIVE_STR_SML => builder.withNodeProperties(randPropertyFor(columnDefinition.split("-")(1), PROPERTY))
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val table = SemanticTable()
      .addNode(astVariable(NODE_A))
      .addNode(astVariable(NODE_B))
    Distinct_columns match {
      case ONE_COLUMN_PRIMITIVE =>
        val allNodesScanA = plans.AllNodesScan(NODE_A, Set.empty)(IdGen)
        val allNodesScanB = plans.AllNodesScan(NODE_B, Set.empty)(IdGen)
        val cartesianProduct = plans.CartesianProduct(allNodesScanA, allNodesScanB)(IdGen)
        val distinct = plans.Distinct(cartesianProduct, Map(NODE_B -> astVariable(NODE_B)))(IdGen)
        val resultColumns = List(NODE_B)
        val produceResults = plans.ProduceResult(distinct, columns = resultColumns)(IdGen)
        (produceResults, table, resultColumns)

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
        (produceResults, table, resultColumns)

      case TWO_COLUMNS_PRIMITIVE =>
        val allNodesScanA = plans.AllNodesScan(NODE_A, Set.empty)(IdGen)
        val allNodesScanB = plans.AllNodesScan(NODE_B, Set.empty)(IdGen)
        val cartesianProduct = plans.CartesianProduct(allNodesScanA, allNodesScanB)(IdGen)
        val distinct = plans.Distinct(
          cartesianProduct,
          Map(NODE_A -> astVariable(NODE_A), NODE_B -> astVariable(NODE_B)))(IdGen)
        val resultColumns = List(NODE_A, NODE_B)
        val produceResults = plans.ProduceResult(distinct, columns = resultColumns)(IdGen)
        (produceResults, table, resultColumns)

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
        (produceResults, table, resultColumns)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: DistinctThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT_MIN, EXPECTED_ROW_COUNT_MAX, visitor)
  }
}

@State(Scope.Thread)
class DistinctThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: Distinct): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.Distinct_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
