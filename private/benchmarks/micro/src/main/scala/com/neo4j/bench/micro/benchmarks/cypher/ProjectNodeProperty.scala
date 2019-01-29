package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.TypeParamValues._
import com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class ProjectNodeProperty extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var ProjectNodeProperty_runtime: String = _

  @ParamValues(
    allowed = Array(LNG, STR_SML),
    base = Array(LNG, STR_SML))
  @Param(Array[String]())
  var ProjectNodeProperty_type: String = _

  override def description = "MATCH (n) RETURN n.key"

  private val NODE_COUNT = 1000000
  private val KEY = "key"

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withNodeProperties(randPropertyFor(ProjectNodeProperty_type, KEY))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("n")
    val nodeIdName = node.name
    val allNodeScan = plans.AllNodesScan(nodeIdName, Set.empty)(Solved)
    val property = astProperty(node, KEY)
    val projection = plans.Projection(allNodeScan, Map(KEY -> property))(Solved)
    val resultColumns = List(nodeIdName)
    val produceResults = plans.ProduceResult(resultColumns, projection)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ProjectNodePropertyThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(NODE_COUNT, visitor)
  }
}

@State(Scope.Thread)
class ProjectNodePropertyThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: ProjectNodeProperty): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.ProjectNodeProperty_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
