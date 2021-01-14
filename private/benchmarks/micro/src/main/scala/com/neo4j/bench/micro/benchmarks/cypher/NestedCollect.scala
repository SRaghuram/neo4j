/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.data.RelationshipDefinition
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astAnds
import com.neo4j.bench.micro.data.Plans.astHasTypes
import com.neo4j.bench.micro.data.Plans.astListLiteral
import com.neo4j.bench.micro.data.Plans.astNestedCollect
import com.neo4j.bench.micro.data.Plans.astTrue
import com.neo4j.bench.micro.data.Plans.astVariable
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.planner.spi.PlanContext
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
class NestedCollect extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

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

  override def description = "NestedCollect((a)-[r1]->(b) WHERE r1:J)"

  private val NODE_COUNT = 10000
  private val EXPECTED_COUNT = 10
  private val RELATIONSHIP_DEFINITIONS =
    RelationshipDefinition.from("(A:10),(B:10),(C:10),(D:10),(E:10),(F:10),(G:10),(H:10),(I:10),(J:10)")

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIP_DEFINITIONS: _*)
      .isReusableStore(true)
      .withNeo4jConfig(Neo4jConfigBuilder.empty()
        .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val relTypeNames = RELATIONSHIP_DEFINITIONS.map(rel => Plans.astRelTypeName(rel.`type`()))
    val nestedScan = plans.AllNodesScan("a", Set.empty)(IdGen)
    val expand = plans.Expand(nestedScan, "a", OUTGOING, relTypeNames, "b", "r1", ExpandAll)(IdGen)
    val filter = plans.Selection(astAnds(astHasTypes("r1", "J")), expand)(IdGen)
    val lhs = plans.UnwindCollection(plans.Argument()(IdGen), "ignore", astListLiteral((1 to EXPECTED_COUNT).map(_ => astTrue)))(IdGen)
    val projection = plans.Projection(lhs, Map("exists" -> astNestedCollect(filter, astTrue)))(IdGen)
    val resultColumns = List("exists")
    val produceResults = plans.ProduceResult(projection, columns = resultColumns)(IdGen)

    val table = SemanticTable().
      addNode(astVariable("a")).
      addNode(astVariable("b")).
      addRelationship(astVariable("r1"))

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: NestedCollectThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_COUNT, subscriber)
    subscriber.count
  }
}

object NestedCollect {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[NestedCollect], args:_*)
  }
}

@State(Scope.Thread)
class NestedCollectThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: NestedCollect): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}