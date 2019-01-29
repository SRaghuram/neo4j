package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.{ExpressionTypeInfo, SemanticTable, symbols}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class Grouping extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var Grouping_runtime: String = _

  @ParamValues(
    allowed = Array("1", "10000"),
    base = Array("1", "10000"))
  @Param(Array[Int]())
  var Grouping_distinctCount: Int = _

  @ParamValues(
    allowed = Array(STR_SML, LNG, DBL),
    base = Array(STR_SML, LNG))
  @Param(Array[String]())
  var Grouping_type: String = _

  override def description = "Grouping only, e.g., MATCH n RETURN DISTINCT n"

  val VALUE_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(Grouping_type)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.SingleRow()(Solved), "value", parameter)(Solved)
    val nodeGrouping = astVariable("value")
    val groupingExpressions = Map("value" -> nodeGrouping)
    val aggregationExpressions = Map[String, Expression]()
    val aggregation: plans.Aggregation = plans.Aggregation(leaf, groupingExpressions, aggregationExpressions)(Solved)
    val resultColumns = List("value")
    val produceResults: plans.LogicalPlan = plans.ProduceResult(columns = resultColumns, aggregation)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: GroupingThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(Grouping_distinctCount, visitor)
  }
}

@State(Scope.Thread)
class GroupingThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: Grouping): Unit = {
    benchmarkState.params = mapValuesOfList(
      "list",
      randomListOf(benchmarkState.Grouping_type, benchmarkState.VALUE_COUNT, benchmarkState.Grouping_distinctCount))
    executionResult = benchmarkState.buildPlan(from(benchmarkState.Grouping_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
