/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DiscreteGenerator.{Bucket, discrete}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues.{DBL, LNG, STR_BIG, STR_SML}
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder, LabelKeyDefinition, PropertyDefinition}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.{DoNotGetValue, IndexOrderNone, IndexedProperty, SingleQueryExpression}
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(false)
class MultiNodeIndexSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME),
    base = Array(Slotted.NAME, Morsel.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.01"),
    base = Array("0.001"))
  @Param(Array[String]())
  var selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML, STR_BIG),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var propertyType: String = _

  override def description = "Cartesian Product of Multiple Index Seeks"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"

  private val TOLERATED_ROW_COUNT_ERROR = 0.05

  private var expectedRowCount: Double = _
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
    expectedRowCount = Math.pow(NODE_COUNT * selectivity, 2)
    minExpectedRowCount = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
    maxExpectedRowCount = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val a = "a"
    val b = "b"
    val literal = astLiteralFor(computeBuckets(0), propertyType)
    val seekExpression = SingleQueryExpression(literal)
    val indexSeekA = plans.NodeIndexSeek(
      a,
      astLabelToken(LABEL, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)
    val indexSeekB = plans.NodeIndexSeek(
      b,
      astLabelToken(LABEL, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)

    val cartesianProduct = plans.CartesianProduct(indexSeekA, indexSeekB)(IdGen)
    val resultColumns = List(a, b)
    val produceResults = plans.ProduceResult(cartesianProduct, columns = resultColumns)(IdGen)

    val table = SemanticTable().addNode(astVariable(a)).addNode(astVariable(b))

    (produceResults, table, resultColumns)
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
