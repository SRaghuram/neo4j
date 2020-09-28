/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete
import com.neo4j.bench.micro.data.LabelKeyDefinition
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.TypeParamValues.DBL
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.STR_BIG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.graphdb.Label
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

@BenchmarkEnabled(false)
class MultiNodeIndexSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0.0001", "0.001", "0.01"),
    base = Array("0.0001"))
  @Param(Array[String]())
  var selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML, STR_BIG),
    base = Array(STR_SML))
  @Param(Array[String]())
  var propertyType: String = _

  @ParamValues(
    allowed = Array("2", "4", "8"),
    base = Array("2", "4"))
  @Param(Array[String]())
  var indexes: Int = _

  override def description = "Cartesian Product of Multiple Index Seeks"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"

  private val TOLERATED_ROW_COUNT_ERROR = 0.05

  private var minExpectedRowCount: Int = _
  private var maxExpectedRowCount: Int = _

  private def computeBuckets: Array[Bucket] = discreteBucketsFor(propertyType, selectivity, 1 - selectivity)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, discrete(computeBuckets: _*)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    val expectedSingleSeekRowCount = NODE_COUNT * selectivity
    val minSingleSeekRowCount = expectedSingleSeekRowCount - TOLERATED_ROW_COUNT_ERROR * expectedSingleSeekRowCount
    val maxSingleSeekRowCount = expectedSingleSeekRowCount + TOLERATED_ROW_COUNT_ERROR * expectedSingleSeekRowCount
    minExpectedRowCount = Math.pow(minSingleSeekRowCount, indexes).toInt
    maxExpectedRowCount = Math.pow(maxSingleSeekRowCount, indexes).toInt
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val indexCreator: String => plans.NodeIndexSeek = plans.NodeIndexSeek(
      _,
      astLabelToken(LABEL, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      SingleQueryExpression(astLiteralFor(computeBuckets(0), propertyType)),
      Set.empty,
      IndexOrderNone)(IdGen)

    val columns = (0 until indexes).map("n" + _).toList

    val indexSeekA = indexCreator(columns(0))
    val indexSeekB = indexCreator(columns(1))
    val cartesianProduct = plans.CartesianProduct(indexSeekA, indexSeekB)(IdGen)

    val table = SemanticTable()
      .addNode(astVariable(columns(0)))
      .addNode(astVariable(columns(1)))

    val (finalCartesianProduct, finalTable) = addCartesianProducts(cartesianProduct, table, indexCreator, columns.slice(2, columns.size))

    val produceResults = plans.ProduceResult(finalCartesianProduct, columns)(IdGen)

    TestSetup(produceResults, finalTable, columns)
  }

  @scala.annotation.tailrec
  private def addCartesianProducts(lhs: plans.CartesianProduct,
                                   table: SemanticTable,
                                   indexCreator: String => plans.NodeIndexSeek,
                                   names: Seq[String]): (plans.CartesianProduct, SemanticTable) =
    if (names.isEmpty) {
      (lhs, table)
    } else {
      val newLhs = plans.CartesianProduct(lhs, indexCreator(names.head))(IdGen)
      val newTable = SemanticTable().addNode(astVariable(names.head))
      addCartesianProducts(newLhs, newTable, indexCreator, names.tail)
    }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: MultipleNodeIndexSeekThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, subscriber)
  }
}

object MultiNodeIndexSeek {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[MultiNodeIndexSeek])
  }
}

@State(Scope.Thread)
class MultipleNodeIndexSeekThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: MultiNodeIndexSeek): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
