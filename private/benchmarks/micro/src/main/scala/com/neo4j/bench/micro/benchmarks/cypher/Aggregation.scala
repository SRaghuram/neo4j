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
class Aggregation extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var Aggregation_runtime: String = _

  @ParamValues(
    allowed = Array(STR_SML, LNG, DBL),
    base = Array(STR_SML, LNG))
  @Param(Array[String]())
  var Aggregation_type: String = _

  override def description = "Aggregation only, e.g., MATCH (n) RETURN count(n)"

  val VALUE_COUNT = 1000000
  val EXPECTED_ROW_COUNT = 1

  var params: MapValue = _

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val listElementType = cypherTypeFor(Aggregation_type)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.SingleRow()(Solved), "value", parameter)(Solved)
    val groupingExpressions = Map[String, Expression]()
    val aggregationExpressions = Map("count" -> astFunctionInvocation(Count.name, astVariable("value")))
    val aggregation = plans.Aggregation(leaf, groupingExpressions, aggregationExpressions)(Solved)
    val resultColumns = List("count")
    val produceResults = plans.ProduceResult(columns = resultColumns, aggregation)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: AggregationThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(params, threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class AggregationThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: Aggregation): Unit = {
    benchmarkState.params = mapValuesOfList("list", listOf(benchmarkState.Aggregation_type, benchmarkState.VALUE_COUNT))
    executionResult = benchmarkState.buildPlan(from(benchmarkState.Aggregation_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
