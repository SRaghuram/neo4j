/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util.stream.Collectors

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
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
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

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

@BenchmarkEnabled(true)
class DeleteRelationship extends AbstractCypherBenchmark {
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
    allowed = Array("100000"),
    base = Array("100000"))
  @Param(Array[String]())
  // Number of nodes with outgoing relationships (total node count slightly higher)
  var nodeCount: Int = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("10")) // Must be lower than `nodeCount`
  @Param(Array[String]())
  var relationshipsPerNode: Int = _

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

  override def description = "MATCH (n)-[r]-() DELETE r return null"

  override protected def getConfig: DataGeneratorConfig =
  // We create relationships in each iteration because otherwise db needs to be huge and shrink for each iteration
    new DataGeneratorConfigBuilder()
      .withNodeCount(nodeCount + relationshipsPerNode) // We create `relationshipsPerNode` extra nodes to make relation creation easier
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
      .deleteRelationship("r")
      .expand("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: DeleteRelationshipThreadState, bh: Blackhole): Unit = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.queryTransaction, subscriber = subscriber)
    result.consumeAll()
  }
}

@State(Scope.Thread)
class DeleteRelationshipThreadState {
  var executablePlan: ExecutablePlan = _
  var queryTransaction: InternalTransaction = _
  var expectedCount: Int = _

  // "HERE LIES DRAGONS" as quoted from the jmh documentation, we still do this to avoid having to create and commit in the benchmark
  @Setup(Level.Invocation)
  def invocationSetup(benchmark: DeleteRelationship): Unit = {
    if (executablePlan == null) {
      executablePlan = benchmark.buildPlan(from(benchmark.runtime))
      expectedCount = benchmark.relationshipsPerNode * benchmark.nodeCount
    }

    // If database has relationships here it probably means the last iteration didn't delete all
    assertNoRelationships(benchmark.beginInternalTransaction())

    createRelationships(benchmark.beginInternalTransaction(), benchmark.relationshipsPerNode)

    // Run each invocation in it's own transaction to avoid growing transactions
    queryTransaction = benchmark.beginInternalTransaction(benchmark.users(benchmark.user))
  }

  @TearDown(Level.Invocation)
  def invocationTearDown(): Unit = {
    queryTransaction.commit() // See comment in invocationSetup
  }

  private def assertNoRelationships(transaction: InternalTransaction): Unit = {
    val relationshipCount = transaction.getAllRelationships.stream().count()
    transaction.close()
    if (relationshipCount != 0) {
      throw new RuntimeException("Test needs to run on database without any relationships")
    }
  }

  private def createRelationships(transaction: InternalTransaction, relationshipsPerNode: Int): Unit = {
    val relationshipType = RelationshipType.withName("LIKES")
    val nodes = transaction.getAllNodes.stream().collect(Collectors.toList[Node])

    // Create `relationshipsPerNode` relationships for all nodes but the last relationshipsPerNode number of nodes
    // We create extra nodes to account for this so in total this gives nodeCount * relationshipsPerNode relationships
    nodes.asScala.iterator
      .sliding(relationshipsPerNode + 1)
      .foreach { nodeGroup =>
        val nodeGroupIterator = nodeGroup.iterator
        val first = nodeGroupIterator.next()
        nodeGroupIterator.foreach(node => first.createRelationshipTo(node, relationshipType))
      }

    transaction.commit()
  }
}

object DeleteRelationship {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[DeleteRelationship])
  }
}
