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
import com.neo4j.bench.micro.data.Augmenterizer
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.ManagedStore
import com.neo4j.bench.micro.data.Stores
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
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

@BenchmarkEnabled(false)
class OrLabelScan extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0.0", "0.5", "1.0"),
    base = Array("0.0", "0.5", "1.0"))
  @Param(Array[String]())
  var overlap: Double = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false"))
  @Param(Array[String]())
  var ordered: Boolean = _

  override def description = "OrLabelScan"

  private val NODE_COUNT = 1000000
  private var expectedRowCount: Int = _
  private val LABEL_A = Label.label("SampleLabelA")
  private val LABEL_B = Label.label("SampleLabelB")

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
      .build()

  override protected def augmentDataGeneration(): Augmenterizer = new Augmenterizer() {
    override def augment(threads: Int, storeAndConfig: Stores.StoreAndConfig): Unit = {
      val overlapCount = (NODE_COUNT * overlap).toInt
      val singleCount = (NODE_COUNT - overlapCount) / 2
      var aCount = 0
      var bCount = 0

      val db = ManagedStore.newDb(storeAndConfig.store, storeAndConfig.config)
      val tx = db.beginTx
      try {
        val allNodes = tx.getAllNodes
        allNodes.iterator().forEachRemaining {
          node =>
            if (aCount < singleCount) {
              node.addLabel(LABEL_A)
              aCount += 1
            } else if (bCount < singleCount) {
              node.addLabel(LABEL_B)
              bCount += 1
            } else {
              node.addLabel(LABEL_A)
              node.addLabel(LABEL_B)
            }
        }
        tx.commit()
      } finally {
        if (tx != null) tx.close()
      }
      ManagedStore.getManagementService.shutdown()
    }
  }

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    expectedRowCount = NODE_COUNT
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val colName = "n"

    var builder = new BenchmarkSetupPlanBuilder()
      .produceResults(colName)

    builder = if (ordered) {
      builder
        .orderedDistinct(Seq(colName), s"$colName AS $colName")
        .orderedUnion(Seq(Ascending(colName)))
        .|.nodeByLabelScan(colName, LABEL_B.name(), IndexOrderAscending)
        .nodeByLabelScan(colName, LABEL_A.name(), IndexOrderAscending)
    } else {
      builder
        .distinct(s"$colName AS $colName")
        .union()
        .|.nodeByLabelScan(colName, LABEL_B.name(), IndexOrderNone)
        .nodeByLabelScan(colName, LABEL_A.name(), IndexOrderNone)
    }

    builder.build()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: OrLabelScanThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(expectedRowCount, subscriber)
  }
}

object OrLabelScan {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[OrLabelScan], args: _*)
  }
}

@State(Scope.Thread)
class OrLabelScanThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: OrLabelScan): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
