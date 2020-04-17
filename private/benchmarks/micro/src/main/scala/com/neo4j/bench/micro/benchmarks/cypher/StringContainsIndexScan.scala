/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.ConstantGenerator.constant
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete
import com.neo4j.bench.micro.data.LabelKeyDefinition
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.Pos
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.TypeParamValues.STR_BIG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.ValueGeneratorUtil.calculateCumulativeSelectivities
import com.neo4j.bench.micro.data.ValueGeneratorUtil.middlePad
import com.neo4j.bench.micro.data.ValueGeneratorUtil.stringLengthFor
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
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

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.asScalaBufferConverter

@BenchmarkEnabled(true)
class StringContainsIndexScan extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0.0001", "0.001", "0.01", "0.1"),
    base = Array("0.0001", "0.1"))
  @Param(Array[String]())
  var selectivity: Double = _

  @ParamValues(
    allowed = Array(STR_SML, STR_BIG),
    base = Array(STR_SML, STR_BIG))
  @Param(Array[String]())
  var propertyType: String = _

  override def description = "String Contains With Index Scan"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"
  private val SELECTIVITIES = List[Double](0.00001, 0.00010, 0.00100, 0.01000, 0.10000)

  private val TOLERATED_ROW_COUNT_ERROR = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  private def stringForRemainder(length: Int): String =
    middlePad("1", '0', length)

  private def stringForSelectivity(length: Int, selectivity: scala.Double): String =
    middlePad(substringForSelectivity(selectivity), '0', length)

  private def substringForSelectivity(selectivity: scala.Double): String = selectivity match {
    case 0.10000 => "11"
    case 0.01000 => "111"
    case 0.00100 => "1111"
    case 0.00010 => "11111"
    case 0.00001 => "111111"
    case _ => throw new IllegalArgumentException(s"Invalid selectivity: $selectivity")
  }

  override protected def getConfig: DataGeneratorConfig = {
    val selectivitiesAsJava = new util.ArrayList[java.lang.Double](SELECTIVITIES.map(d => new java.lang.Double(d)).asJava)
    val cumulativeSelectivities = calculateCumulativeSelectivities(selectivitiesAsJava).asScala.map(d => d.toDouble)
    val length = stringLengthFor(propertyType)
    val buckets = SELECTIVITIES.indices.map(i => new Bucket(
      cumulativeSelectivities(i),
      constant(propertyType, stringForSelectivity(length, SELECTIVITIES(i)))))
    val bucketsWithRemainder = buckets :+ new Bucket(
      1.0 - cumulativeSelectivities.sum,
      constant(propertyType, stringForRemainder(length)))
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, discrete(bucketsWithRemainder: _*)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val value = substringForSelectivity(selectivity)
    val literal = StringLiteral(value)(Pos)
    val indexSeek = plans.NodeIndexContainsScan(
      node.name,
      astLabelToken(LABEL, planContext),
      IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue),
      literal,
      Set.empty,
      IndexOrderNone)(IdGen)

    val resultColumns = List(node.name)
    val produceResults = plans.ProduceResult(indexSeek, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: StringContainsIndexScanThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class StringContainsIndexScanThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _


  @Setup
  def setUp(benchmarkState: StringContainsIndexScan): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
