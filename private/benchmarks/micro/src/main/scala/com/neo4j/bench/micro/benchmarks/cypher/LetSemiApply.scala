/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete
import com.neo4j.bench.micro.data.LabelKeyDefinition
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.VirtualValues
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
class LetSemiApply extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1000"),
    base = Array("1000"))
  @Param(Array[String]())
  var lhsRows: Int = _

  @ParamValues(
    allowed = Array("0", "1", "1000"),
    base = Array("0", "1000"))
  @Param(Array[String]())
  var rhsRows: Int = _

  override def description = "Let Semi Apply"

  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"
  private val PROPERTY_TYPE = LNG
  private lazy val SELECTIVITY = rhsRows/lhsRows
  private val TOLERATED_ROW_COUNT_ERROR = 0.05

  private lazy val buckets: Array[Bucket] = discreteBucketsFor(PROPERTY_TYPE, SELECTIVITY, 1 - SELECTIVITY)
  private lazy val searchLiteral = if(rhsRows == 0) astLiteralFor(System.currentTimeMillis(), LNG) else astLiteralFor(buckets(0).value(), LNG)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(lhsRows)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, discrete(buckets: _*)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    super.afterDatabaseStart(config)
    val tx = db().beginTx()
    val result = tx.execute(s"MATCH (n:$LABEL) WHERE n.$KEY=${searchLiteral.value} RETURN count(*) AS count")
    val value = result.next().get("count").asInstanceOf[Number].intValue()
    if (rhsRows == 0) {
      assert(value == 0)
    } else {
      assert(value >= rhsRows - TOLERATED_ROW_COUNT_ERROR * lhsRows && value <= rhsRows + TOLERATED_ROW_COUNT_ERROR * lhsRows)
    }
    tx.close()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val lhs = "lhs"
    val idName = "idName"
    val lhsAllNodesScan = plans.AllNodesScan(lhs, Set.empty)(IdGen)
    val node = astVariable("node")
    val seekExpression = SingleQueryExpression(searchLiteral)
    val indexSeek = plans.NodeIndexSeek(
      node.name,
      astLabelToken(LABEL, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)

    val letSemiApply = plans.LetSemiApply(lhsAllNodesScan, indexSeek, idName)(IdGen)
    val resultColumns = List(lhs, idName)
    val produceResults = plans.ProduceResult(letSemiApply, columns = resultColumns)(IdGen)

    val table = SemanticTable()
      .addNode(astVariable(lhs))
      .addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: LetSemiApplyThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(VirtualValues.EMPTY_MAP, tx = threadState.tx, subscriber)
    result.consumeAll()
    assertExpectedRowCount(lhsRows, subscriber)
  }
}

@State(Scope.Thread)
class LetSemiApplyThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: LetSemiApply): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object LetSemiApply {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[LetSemiApply])
  }
}
