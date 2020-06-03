/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.TxBatchWithSecurity
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb.GraphDatabaseService
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
class CreateNodeWithLabel extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME)
  )
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1", "10", "100", "1000", "10000"),
    base = Array("1", "100")
  )
  @Param(Array[String]())
  var txSize: Int = _

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

  override def description = "Create node with label"

  private val LABEL = Label.label("A")

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withLabels(LABEL)
      .withNodeCount(1)
      .isReusableStore(false)
      .withNeo4jConfig(Neo4jConfigBuilder.empty()
        .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val createNode = ir.CreateNode("a", Seq(LabelName("A")(InputPosition.NONE)), None)
    val create = plans.Create(plans.Argument()(IdGen), Seq(createNode), Seq.empty)(IdGen)
    val empty = plans.EmptyResult(create)(IdGen)

    (empty, SemanticTable(), List.empty)
  }

  var subscriber: CountSubscriber = _

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def executePlan(threadState: CreateNodeWithLabelThreadState, bh: Blackhole): Unit = {
    if ( null == subscriber ) subscriber = new CountSubscriber(bh)
    threadState.advance()
    val result = threadState.executablePlan.execute(tx = threadState.transaction(), subscriber = subscriber)
    result.consumeAll()
  }

  def database(): GraphDatabaseService = db()
}

@State(Scope.Thread)
class CreateNodeWithLabelThreadState {
  var txBatch: TxBatchWithSecurity = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: CreateNodeWithLabel): Unit = {
    txBatch = new TxBatchWithSecurity(benchmarkState.database(), benchmarkState.txSize, benchmarkState.users(benchmarkState.user))
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
  }

  def advance(): Unit = txBatch.advance()

  def transaction(): InternalTransaction = txBatch.transaction()

  @TearDown
  def tearDown(): Unit = {
    txBatch.close()
  }
}

object CreateNodeWithLabel {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[CreateNodeWithLabel])
  }
}
