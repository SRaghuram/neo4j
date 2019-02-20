/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.ConstantGenerator.constant
import com.neo4j.bench.micro.data.DiscreteGenerator.{Bucket, discrete}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.{INT, LNG}
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class NodeHashJoin extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var NodeHashJoin_runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.01", "0.1"),
    base = Array("0.01"))
  @Param(Array[String]())
  var NodeHashJoin_selectivity: Double = _

  override def description = "Node Hash Join"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"
  private val VALUE = 42
  private val TOLERATED_ROW_COUNT_ERROR = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * NodeHashJoin_selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  override protected def getConfig: DataGeneratorConfig = {
    val buckets = List(
      new Bucket(NodeHashJoin_selectivity, constant(LNG, VALUE)),
      new Bucket(1 - NodeHashJoin_selectivity, constant(INT, 1)))
    val property = new PropertyDefinition(KEY, discrete(buckets: _*))
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(property)
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val nodeIdName = node.name
    val queryExpression = SingleQueryExpression(SignedDecimalIntegerLiteral(VALUE.toString)(Pos))
    val lhs = plans.NodeIndexSeek(
      nodeIdName,
      astLabelToken(LABEL, planContext),
      List(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      queryExpression,
      Set.empty,
      IndexOrderNone)(IdGen)
    val rhs = plans.AllNodesScan(nodeIdName, Set.empty)(IdGen)
    val join = NodeHashJoin(Set(nodeIdName), lhs, rhs)(IdGen)
    val resultColumns = List(node.name)
    val produceResults = ProduceResult(join, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: NodeHashJoinThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, visitor)
  }
}

@State(Scope.Thread)
class NodeHashJoinThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: NodeHashJoin): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.NodeHashJoin_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
