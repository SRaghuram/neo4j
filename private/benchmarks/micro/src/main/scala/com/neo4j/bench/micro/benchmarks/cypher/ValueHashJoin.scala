/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

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
import com.neo4j.bench.micro.data.Plans.astProperty
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.ValueGeneratorUtil.INT
import com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
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

@BenchmarkEnabled(true)
class ValueHashJoin extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.01", "0.1"),
    base = Array("0.01"))
  @Param(Array[String]())
  var selectivity: Double = _

  override def description = "Value Hash Join"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"
  private val VALUE = 42
  private val TOLERATED_ROW_COUNT_ERROR = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  override protected def getConfig: DataGeneratorConfig = {
    val buckets = List(
      new Bucket(selectivity, constant(LNG, VALUE)),
      new Bucket(1 - selectivity, constant(INT, 1)))
    val property = new PropertyDefinition(KEY, discrete(buckets: _*))
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(property)
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val nodeLeft = astVariable("nodeLeft")
    val nodeRight = astVariable("nodeRight")
    val queryExpression = SingleQueryExpression(SignedDecimalIntegerLiteral(VALUE.toString)(Pos))
    val lhs = plans.NodeIndexSeek(
      nodeLeft.name,
      astLabelToken(LABEL, planContext),
      List(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      queryExpression,
      Set.empty,
      IndexOrderNone)(IdGen)
    val rhs = plans.AllNodesScan(nodeRight.name, Set.empty)(IdGen)
    val join = ValueHashJoin(lhs, rhs, Equals(astProperty(nodeLeft, KEY), astProperty(nodeLeft, KEY))(Pos))(IdGen)
    val resultColumns = List(nodeLeft.name)
    val produceResults = ProduceResult(join, resultColumns)(IdGen)

    val table = SemanticTable().addNode(nodeLeft)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ValueHashJoinThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class ValueHashJoinThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: ValueHashJoin): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
