/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.ConstantGenerator.constant
import com.neo4j.bench.micro.data.DiscreteGenerator.{Bucket, discrete}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.{calculateCumulativeSelectivities, prefixPad, stringLengthFor}
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.expressions.StringLiteral
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._

@BenchmarkEnabled(true)
class StringEndsWithIndexScan extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(Interpreted.NAME))
  @Param(Array[String]())
  var StringEndsWithIndexScan_runtime: String = _

  @ParamValues(
    allowed = Array("0.0001", "0.001", "0.01", "0.1"),
    base = Array("0.0001", "0.1"))
  @Param(Array[String]())
  var StringEndsWithIndexScan_selectivity: Double = _

  @ParamValues(
    allowed = Array(STR_SML, STR_BIG),
    base = Array(STR_SML, STR_BIG))
  @Param(Array[String]())
  var StringEndsWithIndexScan_type: String = _

  override def description = "String Ends With Index Scan"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"
  private val SELECTIVITIES = List[Double](0.00001, 0.00010, 0.00100, 0.01000, 0.10000)

  private val TOLERATED_ROW_COUNT_ERROR = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * StringEndsWithIndexScan_selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  private def stringForRemainder(length: Int): String =
    prefixPad("1", '0', length)

  private def stringForSelectivity(length: Int, selectivity: scala.Double): String =
    prefixPad(suffixForSelectivity(selectivity), '0', length)

  private def suffixForSelectivity(selectivity: scala.Double): String = selectivity match {
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
    val length = stringLengthFor(StringEndsWithIndexScan_type)
    val buckets = SELECTIVITIES.indices.map(i => new Bucket(
      cumulativeSelectivities(i),
      constant(StringEndsWithIndexScan_type, stringForSelectivity(length, SELECTIVITIES(i)))))
    val bucketsWithRemainder = buckets :+ new Bucket(
      1.0 - cumulativeSelectivities.sum,
      constant(StringEndsWithIndexScan_type, stringForRemainder(length)))
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
    val value = suffixForSelectivity(StringEndsWithIndexScan_selectivity)
    val literal = StringLiteral(value)(Pos)
    val indexSeek = plans.NodeIndexEndsWithScan(
      node.name,
      astLabelToken(LABEL, planContext),
      astPropertyKeyToken(KEY, planContext),
      literal,
      Set.empty)(IdGen)

    val resultColumn = List(node.name)
    val produceResults = plans.ProduceResult(indexSeek, resultColumn)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumn)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: StringEndsWithIndexScanThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, visitor)
  }
}

@State(Scope.Thread)
class StringEndsWithIndexScanThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: StringEndsWithIndexScan): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.StringEndsWithIndexScan_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
