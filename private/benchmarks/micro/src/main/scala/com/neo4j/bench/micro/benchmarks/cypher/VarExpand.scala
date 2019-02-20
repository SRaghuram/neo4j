/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete
import com.neo4j.bench.micro.data.Plans.{astLiteralFor, _}
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v3_5.expressions.{RelTypeName, SemanticDirection, True}
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.util.symbols
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class VarExpand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var VarExpand_runtime: String = _

  @ParamValues(
    allowed = Array("10"),
    base = Array("10"))
  @Param(Array[String]())
  var VarExpand_degree: Int = _

  @ParamValues(
    allowed = Array("1", "2"),
    base = Array("2"))
  @Param(Array[String]())
  var VarExpand_minDepth: Int = _

  @ParamValues(
    allowed = Array("1", "2", "3"),
    base = Array("3"))
  @Param(Array[String]())
  var VarExpand_length: Int = _

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
    .withOutRelationships(new RelationshipDefinition(relType, VarExpand_degree))
    .isReusableStore(true)
    .build()

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
      VarPatternLength(VarExpand_minDepth, Some(VarExpand_minDepth + VarExpand_length)),
      ExpandAll,
      "r_NODES",
      "r_RELS",
      nodePredicate = True()(Pos),
      edgePredicate = True()(Pos),
      Seq.empty)(IdGen)

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
  def executePlan(threadState: VarExpandThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    visitor.count
  }
}

@State(Scope.Thread)
class VarExpandThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: VarExpand): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.VarExpand_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
