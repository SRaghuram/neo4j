/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util
import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.data.ArrayGenerator.intArray
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.data.NumberGenerator.randInt
import com.neo4j.bench.data.Plans.IdGen
import com.neo4j.bench.data.Plans.astParameter
import com.neo4j.bench.data.Plans.astVariable
import com.neo4j.bench.data.RelationshipDefinition
import com.neo4j.bench.data.ValueGeneratorFun
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
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
class DirectedRelationshipByIdSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1", "10", "100"),
    base = Array("1"))
  @Param(Array[String]())
  var numIDs: Int = _

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

  override def description = "Directed Relationship By ID Seek"

  val RELATIONSHIPS_PER_NODE: Int = 1
  val NODE_COUNT: Int = 1000000
  val RELATIONSHIP_COUNT: Int = NODE_COUNT * RELATIONSHIPS_PER_NODE
  private val RELATIONSHIP_DEFINITION = new RelationshipDefinition(RelationshipType.withName("REL"), RELATIONSHIPS_PER_NODE)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIP_DEFINITION)
      .isReusableStore(true)
      .withNeo4jConfig(Neo4jConfigBuilder.empty()
        .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val cols = List("a", "rel", "b")

    val seek = plans.DirectedRelationshipByIdSeek("rel", ManySeekableArgs(astParameter("ids", symbols.CTList(symbols.CTInteger))), "a", "b", Set.empty)(IdGen)
    val produceResults = plans.ProduceResult(seek, columns = cols)(IdGen)

    val table = SemanticTable().addNode(astVariable("a")).addNode(astVariable("b")).addRelationship(astVariable("rel"))

    TestSetup(produceResults, table, cols)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: DirectedRelationshipByIdThreadState, bh: Blackhole, rngState: RNGState): Long = {
    val idsToSeek = threadState.paramsGen.next(rngState.rng)
    val idList = util.Arrays.asList(idsToSeek: _*)
    val paramMap = util.Collections.singletonMap("ids", idList)
    val params = ValueUtils.asMapValue(paramMap)

    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(numIDs, subscriber)
  }
}

@State(Scope.Thread)
class DirectedRelationshipByIdThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var paramsGen: ValueGeneratorFun[Array[Int]] = _

  @Setup
  def setUp(benchmarkState: DirectedRelationshipByIdSeek): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    paramsGen = intArray(randInt(0, benchmarkState.RELATIONSHIP_COUNT - 1), benchmarkState.numIDs).create()
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
