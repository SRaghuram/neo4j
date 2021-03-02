/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.data.DiscreteGenerator.discrete
import com.neo4j.bench.data.LabelKeyDefinition
import com.neo4j.bench.data.PropertyDefinition
import com.neo4j.bench.data.RelationshipDefinition
import com.neo4j.bench.data.ValueGeneratorUtil.discreteBucketsFor
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.Pos
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
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

@BenchmarkEnabled(true)
class VarExpand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("10"),
    base = Array("10"))
  @Param(Array[String]())
  var degree: Int = _

  @ParamValues(
    allowed = Array("1", "2"),
    base = Array("2"))
  @Param(Array[String]())
  var minDepth: Int = _

  @ParamValues(
    allowed = Array("1", "2", "3"),
    base = Array("3"))
  @Param(Array[String]())
  var length: Int = _

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

  override def description = "MATCH (a:Label{k1:42})-[:*1..3]->(b) RETURN a,b"

  private val nodeCount = 100000
  private val label = Label.label("Label")
  private val lookupKey = "lookup"
  private val lookupSelectivity = 0.01
  private val lookupDistribution = discreteBucketsFor(LNG, lookupSelectivity, 1 - lookupSelectivity)
  private val relType = RelationshipType.withName("REL")

  override protected def getConfig: DataGeneratorConfig = new DataGeneratorConfigBuilder()
    .withNodeCount(nodeCount)
    .withLabels(label)
    .withNodeProperties(new PropertyDefinition(lookupKey, discrete(lookupDistribution: _*)))
    .withSchemaIndexes(new LabelKeyDefinition(label, lookupKey))
    .withOutRelationships(new RelationshipDefinition(relType, degree))
    .isReusableStore(true)
    .withNeo4jConfig(Neo4jConfigBuilder.empty()
      .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
    .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val startNode = astVariable("start")
    val endNode = astVariable("end")
    val rel = astVariable("rel")
    val startNodeName = startNode.name
    val endNodeName = endNode.name
    val relName = rel.name

    val labelToken = astLabelToken(label, planContext)
    val keyToken = IndexedProperty(astPropertyKeyToken(lookupKey, planContext), DoNotGetValue)
    val seekExpression = SingleQueryExpression(astLiteralFor(lookupDistribution(0), LNG))
    val indexSeek = plans.NodeIndexSeek(
      startNodeName,
      labelToken,
      Seq(keyToken),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)

    val expand = plans.VarExpand(
      indexSeek,
      startNodeName,
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      Seq(RelTypeName(relType.name())(Pos)),
      endNodeName,
      relName,
      VarPatternLength(minDepth, Some(minDepth + length)),
      ExpandAll,
      nodePredicate = None,
      relationshipPredicate = None)(IdGen)

    val resultColumns = List(startNode.name, endNode.name)
    val produceResults = plans.ProduceResult(expand, columns = resultColumns)(IdGen)

    val nodesTable = SemanticTable()
      .addNode(startNode)
      .addNode(endNode)
    val table = nodesTable.copy(types = nodesTable.types.updated(rel, ExpressionTypeInfo(symbols.CTList(symbols.CTRelationship), None)))

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: VarExpandThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    subscriber.count
  }
}

@State(Scope.Thread)
class VarExpandThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: VarExpand): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
