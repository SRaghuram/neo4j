/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.security

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.benchmarks.cypher._
import com.neo4j.bench.micro.data.Plans.{IdGen, astVariable}
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class SchemaLevelAllNodesScan extends AbstractSecurityBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Morsel.NAME))
  @Param(Array[String]())
  var SchemaLevelAllNodesScan_runtime: String = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full", "white", "black")
  )
  @Param(Array[String]())
  var SchemaLevelAllNodesScan_user: String = _

  override def description = "All Nodes Scan"

  val NODE_COUNT = 1000000
  val EXPECTED_ROW_COUNT: Int = NODE_COUNT
  private val label = Label.label("Label")

  override protected def getConfig: DataGeneratorConfig = {
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(label)
      .isReusableStore(false)
      .withNeo4jConfig( neo4jConfig )
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = "node"
    val allNodesScan = plans.AllNodesScan(node, Set.empty)(IdGen)
    val resultColumns = List(node)
    val produceResults = plans.ProduceResult(allNodesScan, columns = resultColumns)(IdGen)

    val table = SemanticTable().addNode(astVariable(node))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AllNodesScanThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class AllNodesScanThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: SchemaLevelAllNodesScan): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.SchemaLevelAllNodesScan_runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.SchemaLevelAllNodesScan_user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object SchemaLevelAllNodesScan {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[SchemaLevelAllNodesScan], args:_*)
  }
}
