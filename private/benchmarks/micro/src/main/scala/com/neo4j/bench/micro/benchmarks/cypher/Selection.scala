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
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astEquals
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astProperty
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
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
class Selection extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.5", "0.99"),
    base = Array("0.001", "0.5", "0.99"))
  @Param(Array[String]())
  var selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var propertyType: String = _

  override def description = "MATCH (n) WHERE n.key=$val RETURN n"

  private val NODE_COUNT = 1000000
  private val KEY = "key"

  private val TOLERATED_ROW_COUNT_ERROR: Double = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  private lazy val buckets: Array[Bucket] = discreteBucketsFor(propertyType, selectivity, 1 - selectivity)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withNodeProperties(new PropertyDefinition(KEY, discrete(buckets: _*)))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("n")
    val nodeIdName = node.name
    val allNodeScan = plans.AllNodesScan(nodeIdName, Set.empty)(IdGen)
    val property = astProperty(node, KEY)
    val literalValue = astLiteralFor(buckets(0), propertyType)
    val predicate = astEquals(property, literalValue)
    val selection = plans.Selection(Seq(predicate), allNodeScan)(IdGen)
    val resultColumns = List(nodeIdName)
    val produceResults = plans.ProduceResult(selection, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: SelectionThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class SelectionThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Selection): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
