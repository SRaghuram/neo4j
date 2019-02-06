/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_4.expressions.{Modulo, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_4.functions.Id
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class OptionalExpand extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var OptionalExpand_runtime: String = _

  override def description = "MATCH (n1) OPTIONAL MATCH (n1)-[r:R1]->(n2) WHERE id(n2)%2=0 RETURN n1,r,n2"

  private val RELATIONSHIP_TYPE = RelationshipType.withName("R1")
  private val NODE_COUNT = 1000000
  private val EXPECTED_ROW_COUNT = NODE_COUNT

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(new RelationshipDefinition(RELATIONSHIP_TYPE, 1))
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val n1 = astVariable("n1")
    val r = astVariable("r")
    val n2 = astVariable("n2")
    val two = SignedDecimalIntegerLiteral("2")(Pos)
    val zero = SignedDecimalIntegerLiteral("0")(Pos)

    val allNodesScan = plans.AllNodesScan(n1.name, Set.empty)(IdGen)
    val modulo = Modulo(Id.asInvocation(n2)(Pos), two)(Pos)
    val equals = astEquals(modulo, zero)
    val optionalExpand = plans.OptionalExpand(
      allNodesScan,
      n1.name,
      OUTGOING,
      Seq(astRelTypeName(RELATIONSHIP_TYPE)),
      n2.name,
      r.name,
      plans.ExpandAll,
      Seq(equals))(IdGen)
    val resultColumns = List(n1.name, n2.name, r.name)
    val produceResults = plans.ProduceResult(optionalExpand, columns = resultColumns)(IdGen)

    val table = SemanticTable()
      .addNode(n1)
      .addNode(n2)
      .addRelationship(r)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: OptionalExpandThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executionResult(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(EXPECTED_ROW_COUNT, visitor)
  }
}

@State(Scope.Thread)
class OptionalExpandThreadState {
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _

  @Setup
  def setUp(benchmarkState: OptionalExpand): Unit = {
    executionResult = benchmarkState.buildPlan(from(benchmarkState.OptionalExpand_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
