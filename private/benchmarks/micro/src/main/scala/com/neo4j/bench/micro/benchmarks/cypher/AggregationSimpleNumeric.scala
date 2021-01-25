/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.AggregationHelper.setupAggregation
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.TypeParamValues.DBL
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.mapValuesOfList
import com.neo4j.bench.micro.data.TypeParamValues.shuffledListOf
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

/**
 * Micro benchmarks of aggregation functions that work on numbers and don't take any extra parameters.
 */
@BenchmarkEnabled(true)
class AggregationSimpleNumeric extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("min", "max", "avg", "stDev", "stDevP", "sum"),
    base = Array("min", "max", "avg", "stDev", "stDevP", "sum"))
  @Param(Array[String]())
  var aggregatingFunction: String = _

  @ParamValues(
    allowed = Array(LNG, DBL),
    base = Array(DBL))
  @Param(Array[String]())
  var propertyType: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("false")
  )
  @Param(Array[String]())
  var auth: Boolean = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full")
  )
  @Param(Array[String]())
  var user: String = _

  override def description = "Numerical aggregations, e.g., MATCH (n) RETURN min(n.prop)"

  val VALUE_COUNT = 1000000
  val DISTINCT_COUNT = VALUE_COUNT
  val EXPECTED_ROW_COUNT = 1

  var params: MapValue = _

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .isReusableStore(true)
      .withNeo4jConfig(Neo4jConfigBuilder.empty()
        .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
      .build();

  override def setup(planContext: PlanContext): TestSetup = {
    setupAggregation(propertyType, aggregatingFunction)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AggregationSimpleNumericThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, threadState.tx, subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class AggregationSimpleNumericThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: AggregationSimpleNumeric): Unit = {
    val list = shuffledListOf(benchmarkState.propertyType, benchmarkState.VALUE_COUNT, benchmarkState.DISTINCT_COUNT)
    benchmarkState.params = mapValuesOfList("list", list)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
