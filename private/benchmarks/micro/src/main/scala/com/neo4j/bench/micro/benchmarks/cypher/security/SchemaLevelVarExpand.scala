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
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete
import com.neo4j.bench.micro.data.Plans.{astLiteralFor, _}
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.{RelTypeName, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class SchemaLevelVarExpand extends AbstractSecurityBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Morsel.NAME, Parallel.NAME),
    base = Array(Slotted.NAME, Morsel.NAME))
  @Param(Array[String]())
  var SchemaLevelVarExpand_runtime: String = _

  @ParamValues(
    allowed = Array("1", "2", "5", "10"),
    base = Array("5"))
  @Param(Array[String]())
  var SchemaLevelVarExpand_degree: Int = _

  @ParamValues(
    allowed = Array("1", "2", "4", "10"),
    base = Array("2"))
  @Param(Array[String]())
  var SchemaLevelVarExpand_minDepth: Int = _

  @ParamValues(
    allowed = Array("0","1", "2", "3", "4"),
    base = Array("2", "3", "4"))
  @Param(Array[String]())
  var SchemaLevelVarExpand_length: Int = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full", "white", "black")
  )
  @Param(Array[String]())
  var SchemaLevelVarExpand_user: String = _

  override def description = "MATCH (a:Label{k1:42})-[:*1..3]->(b) RETURN a,b"

  private val nodeCount = 10000
  private val label = Label.label("Label")
  private val lookupKey = "lookup"
  private val lookupSelectivity = 0.01
  private val lookupDistribution = discreteBucketsFor(LNG, lookupSelectivity, 1 - lookupSelectivity)
  private val relType = RelationshipType.withName("REL")

  override protected def getConfig: DataGeneratorConfig = {
    new DataGeneratorConfigBuilder()
      .withNodeCount(nodeCount)
      .withLabels(label)
      .withNodeProperties(new PropertyDefinition(lookupKey, discrete(lookupDistribution: _*)))
      .withSchemaIndexes(new LabelKeyDefinition(label, lookupKey))
      .withOutRelationships(new RelationshipDefinition(relType, SchemaLevelVarExpand_degree))
      .isReusableStore(false)
      .withNeo4jConfig(neo4jConfig)
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
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
      VarPatternLength(SchemaLevelVarExpand_minDepth, Some(SchemaLevelVarExpand_minDepth + SchemaLevelVarExpand_length)),
      ExpandAll,
      nodePredicate = None,
      relationshipPredicate = None)(IdGen)

    val resultColumns = List(startNode.name, endNode.name)
    val produceResults = plans.ProduceResult(expand, columns = resultColumns)(IdGen)

    val nodesTable = SemanticTable()
      .addNode(startNode)
      .addNode(endNode)
    val table = nodesTable.copy(types = nodesTable.types.updated(rel, ExpressionTypeInfo(symbols.CTList(symbols.CTRelationship), None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    subscriber.count
  }
}

@State(Scope.Thread)
class ThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var expectedCount: Int = _

  @Setup
  def setUp(benchmarkState: SchemaLevelVarExpand): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.SchemaLevelVarExpand_runtime))
    tx = benchmarkState.beginInternalTransaction(benchmarkState.users(benchmarkState.SchemaLevelVarExpand_user))
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object SchemaLevelVarExpand {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[SchemaLevelVarExpand], args: _*)
  }
}
