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
import com.neo4j.bench.micro.data.Plans
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.RelationshipDefinition
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.ConfigMemoryTrackingController
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.NoSchedulerTracing
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.graphdb.RelationshipType
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
class OperatorFusionWithExpand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Pipelined.NAME, PipelinedSourceCode.NAME, Parallel.NAME),
    base = Array(Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("4", "8", "16", "32", "64", "128"),
    base = Array("32"))
  @Param(Array[String]())
  var expands: Int = _

  @ParamValues(
    allowed = Array("1", "2", "3", "4", "5", "6", "8", "10", "12", "16", "20", "24", "28", "32", "64", "128"),
    base = Array("1", "2", "4", "8", "12", "16", "32"))
  @Param(Array[String]())
  var limit: Int = _

  override def description = "Lots of single expands with opportunity for operator fusion over pipelines"

  private val NODE_COUNT = 100000
  private val RELATIONSHIP_TYPE = RelationshipType.withName("REL")
  private val RELATIONSHIPS_PER_NODE = new RelationshipDefinition(RELATIONSHIP_TYPE, 1)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIPS_PER_NODE)
      .isReusableStore(true)
      .build()

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
    val allNodesScan = plans.AllNodesScan("n0", Set.empty)(IdGen)
    val relTypeNames = Seq(Plans.astRelTypeName(RELATIONSHIP_TYPE))

    var table = SemanticTable()
    var plan: LogicalPlan = allNodesScan
    var i = 0
    while (i < expands) {
      val n1 = s"n$i"
      val n2 = s"n${i+1}"
      val r = s"r$i"
      plan = plans.Expand(plan, n1, OUTGOING, relTypeNames, n2, r, ExpandAll)(IdGen)
      table = table.addNode(astVariable(n1)).addNode(astVariable(n2)).addRelationship(astVariable(r))
      i += 1
    }
    val resultColumns = List(s"n${i-1}", s"n${i}", s"r${i-1}")
    val produceResults = plans.ProduceResult(plan, resultColumns)(IdGen)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: OperatorFusionWithExpandThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    subscriber.count
  }
}

@State(Scope.Thread)
class OperatorFusionWithExpandThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: OperatorFusionWithExpand): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object OperatorFusionWithExpand {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[OperatorFusionWithExpand])
  }
}
