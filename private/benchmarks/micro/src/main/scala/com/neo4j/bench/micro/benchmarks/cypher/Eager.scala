/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
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
class Eager extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0", "4"),
    base = Array("4"))
  @Param(Array[String]())
  var referenceColumns: Int = _

  @ParamValues(
    allowed = Array("1", "4"),
    base = Array("1", "4"))
  @Param(Array[String]())
  var primitiveColumns: Int = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(STR_SML))
  @Param(Array[String]())
  var refType: String = _

  override def description = "MATCH (n1), (n2) WITH n1, n2, 1 AS r1, 2 AS r2 LIMIT count RETURN n1, n2, r1, r2"

  val NODE_COUNT = 1000000
  val EXPECTED_ROW_COUNT = NODE_COUNT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .isReusableStore(true)
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val nodeName = s"n$primitiveColumns"
    val leftAllNodesScan = plans.AllNodesScan(nodeName, Set.empty)(IdGen)
    val (cartesianProducts, table, nodeNames) = buildCartesianProducts(
      primitiveColumns - 1,
      leftAllNodesScan,
      SemanticTable().addNode(astVariable(nodeName)),
      List(nodeName))
    val limit = plans.Limit(cartesianProducts, astLiteralFor(NODE_COUNT, LNG), DoNotIncludeTies)(IdGen)
    val projectColumns = Range(0, referenceColumns).map(i => (s"r$i", astLiteralFor(i, refType))).toMap
    val projection = plans.Projection(limit, projectColumns)(IdGen)
    val eager = plans.Eager(projection)(IdGen)
    val resultColumns = nodeNames ++ projectColumns.keys.toList
    val produceResult = plans.ProduceResult(eager, columns = resultColumns)(IdGen)
    TestSetup(produceResult, table, resultColumns)
  }

  private def buildCartesianProducts(count: Int, left: LogicalPlan, table: SemanticTable, nodeNames: List[String]): (plans.LogicalPlan, SemanticTable, List[String]) = {
    if (count == 0)
      (left, table, nodeNames)
    else {
      val nodeName = s"n$count"
      val allNodesScan = plans.AllNodesScan(nodeName, Set.empty)(IdGen)
      buildCartesianProducts(
        count - 1,
        plans.CartesianProduct(left, allNodesScan)(IdGen),
        table.addNode(astVariable(nodeName)),
        nodeNames :+ nodeName)
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: EagerThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_ROW_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class EagerThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Eager): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
