/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util

import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.{ascGeneratorFor, randGeneratorFor}
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class UniqueIndexSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var UniqueIndexSeek_runtime: String = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML, STR_BIG, DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION),
    base = Array(DATE_TIME))
  @Param(Array[String]())
  var UniqueIndexSeek_type: String = _

  override def description = "Unique Index Seek"

  val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"
  private val PARAM = "param"
  private val EXPECTED_VALUE_COUNT = 1

  var values: ValueGeneratorFun[_] = _

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, ascGeneratorFor(UniqueIndexSeek_type, 0)))
      .withUniqueConstraints(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val parameter = astParameter(PARAM, symbols.CTAny)
    val seekExpression = SingleQueryExpression(parameter)
    val indexSeek = plans.NodeUniqueIndexSeek(
      node.name,
      astLabelToken(LABEL, planContext),
      Seq(astPropertyKeyToken(KEY, planContext)),
      seekExpression,
      Set.empty)(IdGen)
    val resultColumns = List(node.name)
    val produceResults = ProduceResult(indexSeek, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: UniqueIndexSeekThreadState, rngState: RNGState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    val paramsMap = new util.HashMap[String, Object]()
    // we need instance of to box primitive java types from values
    val value = values.next(rngState.rng).asInstanceOf[AnyRef]
    paramsMap.put(PARAM, value)
    threadState.executionResult(ValueUtils.asMapValue(paramsMap), threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_VALUE_COUNT, visitor)
  }
}

@State(Scope.Thread)
class UniqueIndexSeekThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: UniqueIndexSeek): Unit = {
    benchmarkState.values = randGeneratorFor(benchmarkState.UniqueIndexSeek_type, 0, benchmarkState.NODE_COUNT, true).create()
    executionResult = benchmarkState.buildPlan(from(benchmarkState.UniqueIndexSeek_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
