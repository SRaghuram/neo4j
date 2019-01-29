package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.{asIntegral, randGeneratorFor}
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class RangeIndexSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var RangeIndexSeek_runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.01", "0.1"),
    base = Array("0.001", "0.1"))
  @Param(Array[String]())
  var RangeIndexSeek_selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML, STR_BIG),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var RangeIndexSeek_type: String = _

  override def description = "Range Index Seek"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"

  private val TOLERATED_ROW_COUNT_ERROR = 0.1
  private lazy val expectedRowCount: Double = NODE_COUNT * RangeIndexSeek_selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, randGeneratorFor(RangeIndexSeek_type, 0, NODE_COUNT, true)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val lowerValue = NODE_COUNT / 2
    val offsetForSelectivity = Math.round(NODE_COUNT * RangeIndexSeek_selectivity)
    // pads when type is string. necessary to get number-like ordering.
    val paddedLowerValue = asIntegral(RangeIndexSeek_type, lowerValue)
    val paddedUpperValue = asIntegral(RangeIndexSeek_type, lowerValue + offsetForSelectivity)
    val lower = astLiteralFor(paddedLowerValue, RangeIndexSeek_type)
    val upper = astLiteralFor(paddedUpperValue, RangeIndexSeek_type)
    val seekExpression = astRangeBetweenQueryExpression(lower, upper)
    val indexSeek = plans.NodeIndexSeek(
      node.name,
      astLabelToken(LABEL, planContext),
      Seq(astPropertyKeyToken(KEY, planContext)),
      seekExpression,
      Set.empty)(Solved)
    val resultColumns = List(node.name)
    val produceResults = plans.ProduceResult(resultColumns, indexSeek)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: RangeIndexSeekThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, visitor)
  }
}

@State(Scope.Thread)
class RangeIndexSeekThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: RangeIndexSeek): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.RangeIndexSeek_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
