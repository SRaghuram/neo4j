/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.data.ConstantGenerator.ConstantGeneratorFactory
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.data.LabelKeyDefinition
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.data.PropertyDefinition
import com.neo4j.bench.micro.data.TypeParamValues.INT
import com.neo4j.bench.data.ValueGeneratorFactory
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
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
class AssertSameNode extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Interpreted.NAME, Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false"))
  @Param(Array[String]())
  var keyExist: Boolean = _

  override def description = "Assert Same Node"

  val NODE_COUNT = 1
  private val LABEL1 = Label.label("SampleLabel1")
  private val LABEL2 = Label.label("SampleLabel2")
  private val KEY = "key"
  val PARAM = "param"
  private lazy val EXPECTED_VALUE_COUNT = if (keyExist) 1 else 0
  val key = 0
  val propertyType = INT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL1, LABEL2)
      .withNodeProperties(new PropertyDefinition(KEY, new ConstantGeneratorFactory(propertyType, key).asInstanceOf[ValueGeneratorFactory[_]]))
      .withUniqueConstraints(new LabelKeyDefinition(LABEL1, KEY), new LabelKeyDefinition(LABEL2, KEY))
      .isReusableStore(true)
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val node = astVariable("node")
    val parameter = astParameter(PARAM, symbols.CTAny)
    val seekExpression = SingleQueryExpression(parameter)
    val indexSeek1 = NodeUniqueIndexSeek(
      node.name,
      astLabelToken(LABEL1, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), GetValue)),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)
    val indexSeek2 = NodeUniqueIndexSeek(
      node.name,
      astLabelToken(LABEL2, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), GetValue)),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)
    val assertSameNode = plans.AssertSameNode(node.name, indexSeek1, indexSeek2)(IdGen)
    val resultColumns = List(node.name)
    val produceResults = ProduceResult(assertSameNode, resultColumns)(IdGen)
    val table = SemanticTable().addNode(node)

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AssertSameNodeThreadState, bh: Blackhole): Long = {

    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(threadState.params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(EXPECTED_VALUE_COUNT, subscriber)
  }
}

@State(Scope.Thread)
class AssertSameNodeThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var params: MapValue = _

  @Setup
  def setUp(benchmarkState: AssertSameNode): Unit = {
    val paramsMap = new util.HashMap[String, Object]()
    // we need instance of to box primitive java types from values

    if (benchmarkState.keyExist) {
      paramsMap.put(benchmarkState.PARAM, Values.of(benchmarkState.key))
    } else {
      paramsMap.put(benchmarkState.PARAM, Values.of(benchmarkState.key-1))
    }
    params = ValueUtils.asMapValue(paramsMap)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object AssertSameNode {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[AssertSameNode])
  }
}
