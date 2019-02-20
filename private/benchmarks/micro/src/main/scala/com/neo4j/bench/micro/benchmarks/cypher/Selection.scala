/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.DiscreteGenerator.{Bucket, discrete}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class Selection extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var Selection_runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.5", "0.99"),
    base = Array("0.001", "0.5", "0.99"))
  @Param(Array[String]())
  var Selection_selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var Selection_type: String = _

  override def description = "MATCH (n) WHERE n.key=$val RETURN n"

  private val NODE_COUNT = 1000000
  private val KEY = "key"

  private val TOLERATED_ROW_COUNT_ERROR: Double = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * Selection_selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  private lazy val buckets: Array[Bucket] = discreteBucketsFor(Selection_type, Selection_selectivity, 1 - Selection_selectivity)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withNodeProperties(new PropertyDefinition(KEY, discrete(buckets: _*)))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("n")
    val nodeIdName = node.name
    val allNodeScan = plans.AllNodesScan(nodeIdName, Set.empty)(IdGen)
    val property = astProperty(node, KEY)
    val literalValue = astLiteralFor(buckets(0), Selection_type)
    val predicate = astEquals(property, literalValue)
    val selection = plans.Selection(Seq(predicate), allNodeScan)(IdGen)
    val resultColumns = List(nodeIdName)
    val produceResults = plans.ProduceResult(selection, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: SelectionThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, visitor)
  }
}

@State(Scope.Thread)
class SelectionThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Selection): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.Selection_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
