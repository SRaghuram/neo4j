/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.RelationshipDefinition
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
class Expand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false")
  )
  @Param(Array[String]())
  var auth: Boolean = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full", "white", "black")
  )
  @Param(Array[String]())
  var user: String = _

  override def description = "Expand one step, with many relationship types"

  private val NODE_COUNT = 10000
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

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val relTypeNames = RELATIONSHIP_DEFINITIONS.map(rel => Plans.astRelTypeName(rel.`type`()))
    val allNodesScan = plans.AllNodesScan("a", Set.empty)(IdGen)
    val expand = plans.Expand(allNodesScan, "a", OUTGOING, relTypeNames, "b", "r1", ExpandAll)(IdGen)
    val resultColumns = List("a", "b")
    val produceResults = plans.ProduceResult(expand, columns = resultColumns)(IdGen)

    val table = SemanticTable().
      addNode(astVariable("a")).
      addNode(astVariable("b")).
      addRelationship(astVariable("r1"))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ExpandThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    subscriber.count
  }
}

@State(Scope.Thread)
class ExpandThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Expand): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
