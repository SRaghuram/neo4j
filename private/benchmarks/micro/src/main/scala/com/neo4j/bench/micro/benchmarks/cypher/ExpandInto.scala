/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder, RelationshipDefinition}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class ExpandInto extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledSourceCode.NAME, CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var ExpandInto_runtime: String = _

  override def description = "Expand two steps"

  val NODE_COUNT = 100000
  val RELATIONSHIPS_PER_NODE = new RelationshipDefinition(RelationshipType.withName("REL"), 10)

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIPS_PER_NODE)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val allNodesScan = plans.AllNodesScan("a", Set.empty)(IdGen)
    val expand = plans.Expand(allNodesScan, "a", OUTGOING, Seq.empty, "b", "r1", plans.ExpandAll)(IdGen)
    val expandInto = plans.Expand(expand, "b", INCOMING, Seq.empty, "a", "r2", plans.ExpandInto)(IdGen)
    val resultColumns = List("a", "b")
    val table = SemanticTable().
      addNode(astVariable("a")).
      addNode(astVariable("b")).
      addRelationship(astVariable("r1")).
      addRelationship(astVariable("r2"))


    val produceResults = plans.ProduceResult(expandInto, columns = resultColumns)(IdGen)
    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ExpandIntoThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    visitor.count
  }
}

@State(Scope.Thread)
class ExpandIntoThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: ExpandInto): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.ExpandInto_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
