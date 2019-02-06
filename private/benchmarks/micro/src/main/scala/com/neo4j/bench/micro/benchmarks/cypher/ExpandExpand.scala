/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder, Plans, RelationshipDefinition}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans.ExpandAll
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class ExpandExpand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledSourceCode.NAME, CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var ExpandExpand_runtime: String = _

  override def description = "Expand two steps"

  private val NODE_COUNT = 100000
  private val RELATIONSHIP_TYPE = RelationshipType.withName("REL")
  private val RELATIONSHIPS_PER_NODE = new RelationshipDefinition(RELATIONSHIP_TYPE, 10)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIPS_PER_NODE)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val allNodesScan = plans.AllNodesScan("a", Set.empty)(IdGen)
    val relTypeNames = Seq(Plans.astRelTypeName(RELATIONSHIP_TYPE))
    val exp1 = plans.Expand(allNodesScan, "a", OUTGOING, relTypeNames, "b", "r1", ExpandAll)(IdGen)
    val exp2 = plans.Expand(exp1, "b", OUTGOING, relTypeNames, "c", "r2", ExpandAll)(IdGen)
    val filter = plans.Selection(Seq(astNot(astEquals(astVariable("r1"), astVariable("r2")))), exp2)(IdGen)
    val resultColumns = List("a", "b", "c")
    val produceResults = plans.ProduceResult(filter, resultColumns)(IdGen)

    val table = SemanticTable().
      addNode(astVariable("a")).
      addNode(astVariable("b")).
      addNode(astVariable("c")).
      addRelationship(astVariable("r1")).
      addRelationship(astVariable("r2"))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ExpandExpandThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    visitor.count
  }
}

@State(Scope.Thread)
class ExpandExpandThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: ExpandExpand): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.ExpandExpand_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
