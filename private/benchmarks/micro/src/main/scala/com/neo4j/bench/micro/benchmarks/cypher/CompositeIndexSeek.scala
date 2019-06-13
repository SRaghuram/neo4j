/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGenerator.Order
import com.neo4j.bench.micro.data.DiscreteGenerator.{Bucket, discrete}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexOrderNone
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class CompositeIndexSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var CompositeIndexSeek_runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.01", "0.1"),
    base = Array("0.001", "0.1"))
  @Param(Array[String]())
  var CompositeIndexSeek_selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML, STR_BIG),
    base = Array(LNG))
  @Param(Array[String]())
  var CompositeIndexSeek_type: String = _

  @ParamValues(
    allowed = Array("2", "4"),
    base = Array("2", "4"))
  @Param(Array[String]())
  var CompositeIndexSeek_propertyCount: Int = _

  override def description = "Composite Index Seek"

  private val NODE_COUNT = 1000000
  private val LABEL: Label = Label.label("SampleLabel")

  private val TOLERATED_ROW_COUNT_ERROR: Double = 0.05
  private lazy val expectedRowCount: Double = NODE_COUNT * CompositeIndexSeek_selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  lazy val buckets: Array[Bucket] = {
    val selectivity = Math.pow(CompositeIndexSeek_selectivity, 1.0 / CompositeIndexSeek_propertyCount)
    ValueGeneratorUtil.discreteBucketsFor(CompositeIndexSeek_type, selectivity, 1 - selectivity)
  }

  lazy val properties: Array[PropertyDefinition] =
    Array.range(0, CompositeIndexSeek_propertyCount)
      .map(i => new PropertyDefinition(s"${CompositeIndexSeek_type}_$i", discrete(buckets: _*)))

  lazy val index: LabelKeyDefinition = new LabelKeyDefinition(LABEL, keys: _*)

  lazy val keys: Array[String] = properties.map(_.key())

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(properties: _*)
      .withPropertyOrder(Order.ORDERED)
      .withSchemaIndexes(index)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val nodeIdName = node.name
    val labelToken = astLabelToken(LABEL, planContext)
    // TODO assuming this property key id mapping only works when properties are written in ORDERED order, not SHUFFLED
    // TODO if store was available at this point it would be possible to retrieve this info from the store
    val keyTokens = Seq.range(0, CompositeIndexSeek_propertyCount)
                    .map(i => astPropertyKeyToken(properties(i).key(), planContext))
    val seekExpressions = Seq.range(0, CompositeIndexSeek_propertyCount)
                          .map(_ => SingleQueryExpression(astLiteralFor(buckets(0), CompositeIndexSeek_type)))
    val indexSeek = plans.NodeIndexSeek(
      nodeIdName,
      labelToken,
      keyTokens,
      CompositeQueryExpression(seekExpressions),
      Set.empty,
      IndexOrderNone)(IdGen)
    val resultColumns = List(nodeIdName)
    val produceResults = ProduceResult(indexSeek, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: CompositeIndexSeekThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, visitor)
  }
}

@State(Scope.Thread)
class CompositeIndexSeekThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: CompositeIndexSeek): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.CompositeIndexSeek_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
