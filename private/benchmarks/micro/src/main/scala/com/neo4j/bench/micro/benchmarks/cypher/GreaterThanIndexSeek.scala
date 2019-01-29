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
class GreaterThanIndexSeek extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var GreaterThanIndexSeek_runtime: String = _

  @ParamValues(
    allowed = Array("0.001", "0.01", "0.1"),
    base = Array("0.001", "0.1"))
  @Param(Array[String]())
  var GreaterThanIndexSeek_selectivity: Double = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML, STR_BIG),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var GreaterThanIndexSeek_type: String = _

  override def description = "Greater Than Index Seek"

  private val NODE_COUNT = 1000000
  private val LABEL = Label.label("SampleLabel")
  private val KEY = "key"

  private val TOLERATED_ROW_COUNT_ERROR = 0.075
  private lazy val expectedRowCount: Double = NODE_COUNT * GreaterThanIndexSeek_selectivity
  private lazy val minExpectedRowCount: Int = Math.round(expectedRowCount - TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt
  private lazy val maxExpectedRowCount: Int = Math.round(expectedRowCount + TOLERATED_ROW_COUNT_ERROR * expectedRowCount).toInt

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, randGeneratorFor(GreaterThanIndexSeek_type, 0, NODE_COUNT, true)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val offsetForSelectivity = Math.round(NODE_COUNT * GreaterThanIndexSeek_selectivity)
    val lowerValue = NODE_COUNT - offsetForSelectivity
    // pads when type is string. necessary to get number-like ordering.
    val paddedLowerValue = asIntegral(GreaterThanIndexSeek_type, lowerValue)
    val lower = astLiteralFor(paddedLowerValue, GreaterThanIndexSeek_type)
    val seekExpression = astRangeGreaterThanQueryExpression(lower)
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
  def executePlan(threadState: GreaterThanIndexSeekThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(minExpectedRowCount, maxExpectedRowCount, visitor)
  }
}

@State(Scope.Thread)
class GreaterThanIndexSeekThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: GreaterThanIndexSeek): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.GreaterThanIndexSeek_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
