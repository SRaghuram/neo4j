/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.benchmarks.cypher.plan.builder.BenchmarkSetupPlanBuilder
import com.neo4j.bench.data.Augmenterizer
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.data.LabelKeyDefinition
import com.neo4j.bench.data.ManagedStore
import com.neo4j.bench.data.NumberGenerator.ascInt
import com.neo4j.bench.data.PropertyDefinition
import com.neo4j.bench.data.Stores
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.graphdb.Label
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

import scala.collection.JavaConverters.asScalaIteratorConverter

@BenchmarkEnabled(true)
class VarExpandRingGraph extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Interpreted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("100", "1000", "10000", "100000", "1000000"),
    base = Array("100000"))
  @Param(Array[String]())
  var nodes: Int = _

  override def description = "MATCH (a:A {id: 0})-[*1..]->(b) RETURN DISTINCT a, b"

  private val label = Label.label("A")
  private val relType = RelationshipType.withName("REL")
  private val lookupKey = "id"

  override protected def getConfig: DataGeneratorConfig = new DataGeneratorConfigBuilder()
    .withNodeCount(nodes)
    .withLabels(label)
    .withNodeProperties(new PropertyDefinition(lookupKey, ascInt(0)))
    .withSchemaIndexes(new LabelKeyDefinition(label, lookupKey))
    .isReusableStore(true)
    .build()

  override protected def augmentDataGeneration(): Augmenterizer = {
    // Create ring graph
    (_: Int, storeAndConfig: Stores.StoreAndConfig) => {
      val db = ManagedStore.newDb(storeAndConfig.store, storeAndConfig.config)
      val tx = db.beginTx
      try {
        val nodes = tx.getAllNodes.iterator().asScala.toStream

        nodes.sliding(2).foreach {
          case Seq(a, b) => a.createRelationshipTo(b, relType)
        }

        // Close the ring
        nodes.last.createRelationshipTo(nodes.head, relType)

        tx.commit()
      } finally {
        if (tx != null) tx.close()
      }
      ManagedStore.getManagementService.shutdown()
    }
  }

  override def setup(planContext: PlanContext): TestSetup = {
    new BenchmarkSetupPlanBuilder()
      .produceResults("a", "b")
      .distinct("a as a", "b as b")
      .expand("(a)-[*1..]->(b)", ExpandAll, OUTGOING)
      .nodeIndexOperator("a:A(id = 0)")
      .build()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: VarExpandRingGraphThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(nodes, subscriber)
  }
}

@State(Scope.Thread)
class VarExpandRingGraphThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: VarExpandRingGraph): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object VarExpandRingGraph {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[VarExpandRingGraph])
  }
}
