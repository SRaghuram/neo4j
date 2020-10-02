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
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.Plans.cypherTypeFor
import com.neo4j.bench.micro.data.TypeParamValues.listOf
import com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.ConfigMemoryTrackingController
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.NoSchedulerTracing
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
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

@BenchmarkEnabled(true)
class OperatorFusionWithUnwind extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Pipelined.NAME, PipelinedSourceCode.NAME, Parallel.NAME),
    base = Array(Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("4", "8", "16", "32", "64", "128"),
    base = Array("32"))
  @Param(Array[String]())
  var unwinds: Int = _

  @ParamValues(
    allowed = Array("1", "2", "3", "4", "5", "6", "8", "10", "12", "16", "20", "24", "28", "32", "64", "128"),
    base = Array("1", "2", "4", "8", "12", "16", "32"))
  @Param(Array[String]())
  var limit: Int = _

  override def description = "Lots of single expands with opportunity for operator fusion over pipelines"

  val VALUE_COUNT = 100000

  var params: MapValue = _
  val listElementType = cypherTypeFor(LNG)

  private val memoryTrackingController = new ConfigMemoryTrackingController(Config.defaults())

  override protected def getRuntimeConfig: CypherRuntimeConfiguration = {
    CypherRuntimeConfiguration(
      operatorFusionOverPipelineLimit = limit, // <-- From parameter
      pipelinedBatchSizeSmall = ContextHelper.morselSize,
      pipelinedBatchSizeBig = ContextHelper.morselSize,
      schedulerTracing = NoSchedulerTracing,
      lenientCreateRelationship = false,
      memoryTrackingController = memoryTrackingController,
      enableMonitors = false
    )
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listType = symbols.CTList(listElementType)
    val biglistParameter = astParameter("biglist", listType)
    val onelistParameter = astParameter("onelist", listType)

    val v0 = astVariable("v0")
    val unwind = plans.UnwindCollection(plans.Argument()(IdGen), v0.name, biglistParameter)(IdGen)
    var table = SemanticTable().addVariable(v0)
    var plan: LogicalPlan = unwind

    var i = 0
    while (i < unwinds) {
      i += 1
      val v1 = astVariable(s"v${i}")
      plan = plans.UnwindCollection(plan, v1.name, onelistParameter)(IdGen)
      table = table.addVariable(v1)
    }
    val resultColumns = List(v0.name, s"v${i}")
    val produceResults = plans.ProduceResult(plan, resultColumns)(IdGen)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: OperatorFusionWithUnwindThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(VALUE_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class OperatorFusionWithUnwindThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: OperatorFusionWithUnwind): Unit = {
    benchmarkState.params = ValueUtils.asMapValue(java.util.Map.of("biglist", listOf(LNG, benchmarkState.VALUE_COUNT),
                                                                   "onelist", listOf(LNG, 1)))
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object OperatorFusionWithUnwind {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[OperatorFusionWithUnwind])
  }
}
