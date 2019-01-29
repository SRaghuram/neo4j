package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.functions.Count
import org.neo4j.cypher.internal.frontend.v3_3.{ExpressionTypeInfo, SemanticTable, symbols}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class GroupingAndAggregation extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var GroupingAndAggregation_runtime: String = _

  @ParamValues(
    allowed = Array("1", "10000"),
    base = Array("1", "10000"))
  @Param(Array[Int]())
  var GroupingAndAggregation_distinctCount: Int = _

  @ParamValues(
    allowed = Array(LNG, DBL, STR_SML),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var GroupingAndAggregation_type: String = _

  override def description = "Grouping & Aggregation, e.g., MATCH (n) RETURN n, count(n)"

  val VALUE_COUNT = 1000000

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(GroupingAndAggregation_type)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwind = plans.UnwindCollection(plans.SingleRow()(Solved), unwindVariable.name, parameter)(Solved)
    val groupingExpressions = Map("value" -> unwindVariable)
    val aggregationExpressions = Map("count" -> astFunctionInvocation(Count.name, unwindVariable))
    val aggregation = plans.Aggregation(unwind, groupingExpressions, aggregationExpressions)(Solved)
    val resultColumns = List("value", "count")
    val produceResults = plans.ProduceResult(columns = resultColumns, aggregation)
    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))
    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: GroupingAndAggregationThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(GroupingAndAggregation_distinctCount, visitor)
  }
}

@State(Scope.Thread)
class GroupingAndAggregationThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: GroupingAndAggregation): Unit = {
    benchmarkState.params = mapValuesOfList(
      "list",
      randomListOf(benchmarkState.GroupingAndAggregation_type, benchmarkState.VALUE_COUNT, benchmarkState.GroupingAndAggregation_distinctCount))
    executionResult = benchmarkState.buildPlan(from(benchmarkState.GroupingAndAggregation_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
