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
import org.neo4j.cypher.internal.ir.v4_0.VarPatternLength
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.{RelTypeName, SemanticDirection, True}
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class VarExpandWithPredicates extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var VarExpandWithPredicates_runtime: String = _

  @ParamValues(
    allowed = Array("10"),
    base = Array("10"))
  @Param(Array[String]())
  var VarExpandWithPredicates_degree: Int = _

  @ParamValues(
    allowed = Array("1", "2"),
    base = Array("1", "2"))
  @Param(Array[String]())
  var VarExpandWithPredicates_minDepth: Int = _

  @ParamValues(
    allowed = Array("1", "2", "3"),
    base = Array("1", "2", "3"))
  @Param(Array[String]())
  var VarExpandWithPredicates_length: Int = _

  override def description = "MATCH p=(a:Label{k1:42})-[:*1..3]->(b) WHERE all(n IN nodes(p) WHERE k2.x=0) RETURN a,b"

  private val nodeCount = 100000
  private val label = Label.label("Label")
  private val lookupKey = "lookup"
  private val lookupSelectivity = 0.01
  private val lookupDistribution = discreteBucketsFor(LNG, lookupSelectivity, 1 - lookupSelectivity)
  private val predicateKey = "predicate"
  private val predicateSelectivity = 0.5
  private val predicateDistribution = discreteBucketsFor(LNG, predicateSelectivity, 1 - predicateSelectivity)
  private val relType = RelationshipType.withName("REL")

  override protected def getConfig: DataGeneratorConfig = new DataGeneratorConfigBuilder()
    .withNodeCount(nodeCount)
    .withLabels(label)
    .withNodeProperties(
      new PropertyDefinition(lookupKey, discrete(lookupDistribution: _*)),
      new PropertyDefinition(predicateKey, discrete(predicateDistribution: _*)))
    .withSchemaIndexes(new LabelKeyDefinition(label, lookupKey))
    .withOutRelationships(new RelationshipDefinition(relType, VarExpandWithPredicates_degree))
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

    val tempNode = astVariable("r_NODES")
    val tempNodeProperty = astProperty(tempNode, predicateKey)
    val propertyValue = astLiteralFor(predicateDistribution(0), LNG)
    val tempNodePropertyPredicate = astEquals(tempNodeProperty, propertyValue)

    val expand = plans.VarExpand(
      indexSeek,
      startNodeName,
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      Seq(RelTypeName(relType.name())(Pos)),
      endNodeName,
      relName,
      VarPatternLength(
        min = VarExpandWithPredicates_minDepth,
        max = Some(VarExpandWithPredicates_minDepth + VarExpandWithPredicates_length)),
      ExpandAll,
      tempNode.name,
      "r_RELS",
      nodePredicate = tempNodePropertyPredicate,
      relationshipPredicate = True()(Pos),
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
  def executePlan(threadState: VarExpandVarExpandWithPredicatesThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    visitor.count
  }
}

@State(Scope.Thread)
class VarExpandVarExpandWithPredicatesThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: VarExpandWithPredicates): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.VarExpandWithPredicates_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
