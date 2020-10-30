/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLabelName
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
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
class NodeCountFromCountStore extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true")
  )
  @Param(Array[String]())
  var auth: Boolean = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full", "white", "black")
  )
  @Param(Array[String]())
  var user: String = _

  override def description = "CountFromCountStore, MATCH (n:Label) RETURN count(n)"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  val EXPECTED_ROW_COUNT = 1

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .isReusableStore(true)
      .withNeo4jConfig(Neo4jConfigBuilder.empty()
        .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val countFromCountStore = plans.NodeCountFromCountStore("count(n)", List(Some(astLabelName(LABEL))), Set.empty)(IdGen)
    val resultColumns = List("count(n)")
    val produceResults = plans.ProduceResult(countFromCountStore, columns = resultColumns)(IdGen)

    val table = SemanticTable()

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: NodeCountFromCountStoreThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class NodeCountFromCountStoreThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: NodeCountFromCountStore): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
