/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.benchmarks.cypher.plan.builder.BenchmarkSetupPlanBuilder
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class DeleteNode extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME)
  )
  @Param(Array[String]())
  var runtime: String = _

  /*
   * Needs to be high to make each invocation long running because we use invocation
   * level setup (see documentation of org.openjdk.jmh.annotations.Level.Invocation).
   */
  @ParamValues(
    allowed = Array("1000000"),
    base = Array("1000000"))
  @Param(Array[String]())
  var deleteCount: Int = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true")
  )
  @Param(Array[String]())
  var auth: Boolean = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full")
  )
  @Param(Array[String]())
  var user: String = _

  override def description = "MATCH (n) DELETE n return null;"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      // We create nodes in each iteration because otherwise db needs to be huge and shrink on each iteration
      .withNodeCount(0)
      .isReusableStore(false)
      .withNeo4jConfig(
        Neo4jConfigBuilder.empty()
          .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString)
          .build()
      )
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    new BenchmarkSetupPlanBuilder()
      .produceResults()
      .emptyResult()
      .deleteNode("n")
      .allNodeScan("n")
      .build(readOnly = false)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: DeleteNodeThreadState, bh: Blackhole): Unit = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.queryTransaction, subscriber = subscriber)
    result.consumeAll()
  }
}

@State(Scope.Thread)
class DeleteNodeThreadState {
  var executablePlan: ExecutablePlan = _
  var queryTransaction: InternalTransaction = _

  // "HERE LIES DRAGONS" as quoted from the jmh documentation, we still do this to avoid having to create and commit in the benchmark
  @Setup(Level.Invocation)
  def invocationSetup(benchmark: DeleteNode): Unit = {
    if (executablePlan == null) {
      executablePlan = benchmark.buildPlan(from(benchmark.runtime))
    }

    // If database is not empty here it probably means the last iteration didn't delete all nodes
    assertEmptyDatabase(benchmark.beginInternalTransaction())

    createNodes(benchmark.beginInternalTransaction(), benchmark.deleteCount)
    // Run each invocation in it's own transaction to avoid growing transactions
    queryTransaction = benchmark.beginInternalTransaction(benchmark.users(benchmark.user))
  }

  @TearDown(Level.Invocation)
  def invocationTearDown(): Unit = {
    queryTransaction.commit() // See comment in invocationSetup
  }

  private def assertEmptyDatabase(transaction: InternalTransaction): Unit = {
    val nodeCount = transaction.getAllNodes.stream().count()
    transaction.close()
    if (nodeCount != 0) {
      throw new RuntimeException("Test needs to run on empty database")
    }
  }

  private def createNodes(transaction: InternalTransaction, count: Int): Unit = {
    var i = 0
    while (i < count) {
      transaction.createNode()
      i += 1
    }
    transaction.commit()
  }
}

object DeleteNode {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[DeleteNode])
  }
}
