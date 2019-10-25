/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder, Plans, RelationshipDefinition}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans.ExpandAll
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class Expand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledSourceCode.NAME, CompiledByteCode.NAME, Interpreted.NAME, Slotted.NAME, Morsel.NAME),
    base = Array(CompiledByteCode.NAME, Interpreted.NAME, Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("false"))
  @Param(Array[String]())
  var auth: Boolean = _

  @ParamValues(
    allowed = Array("full", "while", "black"),
    base = Array("full"))
  @Param(Array[String]())
  var user: String = _

  override def description = "Expand one step, with many relationship types"

  private val NODE_COUNT = 10000
  private val RELATIONSHIP_DEFINITIONS =
    RelationshipDefinition.from("(A:10),(B:10),(C:10),(D:10),(E:10),(F:10),(G:10),(H:10),(I:10),(J:10)")

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIP_DEFINITIONS: _*)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val relTypeNames = RELATIONSHIP_DEFINITIONS.map(rel => Plans.astRelTypeName(rel.`type`()))
    val allNodesScan = plans.AllNodesScan("a", Set.empty)(IdGen)
    val expand = plans.Expand(allNodesScan, "a", OUTGOING, relTypeNames, "b", "r1", ExpandAll)(IdGen)
    val resultColumns = List("a", "b")
    val produceResults = plans.ProduceResult(expand, columns = resultColumns)(IdGen)

    val table = SemanticTable().
                addNode(astVariable("a")).
                addNode(astVariable("b")).
                addRelationship(astVariable("r1"))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ExpandThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    visitor.count
  }
}

@State(Scope.Thread)
class ExpandThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Expand): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
