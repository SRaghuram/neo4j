/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.{AbstractCypherBenchmark, EnterpriseInterpreted, ExecutablePlan}
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder, Plans, RelationshipDefinition}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v4_0.expressions.{NilPathStep, NodePathStep, SingleRelationshipPathStep}
import org.neo4j.cypher.internal.v4_0.logical._
import org.neo4j.cypher.internal.v4_0.logical.plans.ExpandAll
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class PathExpression extends AbstractCypherBenchmark {

  @ParamValues(
    allowed = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME),
    base = Array(CompiledExpressionEngine.NAME, InterpretedExpressionEngine.NAME))
  @Param(Array[String]())
  var PathExpression_engine: String = _

  override def description = "Path expression over a two-step expand"

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
    val resultColumns = List("result")
    val allNodesScan = plans.AllNodesScan("a", Set.empty)(IdGen)
    val relTypeNames = Seq(Plans.astRelTypeName(RELATIONSHIP_TYPE))
    val exp1 = plans.Expand(allNodesScan, "a", OUTGOING, relTypeNames, "b", "r1", ExpandAll)(IdGen)
    val exp2 = plans.Expand(exp1, "b", OUTGOING, relTypeNames, "c", "r2", ExpandAll)(IdGen)
    val filter = plans.Selection(Seq(astNot(astEquals(astVariable("r1"), astVariable("r2")))), exp2)(IdGen)
    val expression = astFunctionInvocation("length",
                                           astPathExpression(
                                             NodePathStep(astVariable("a"),
                                                          SingleRelationshipPathStep(astVariable("r1"), OUTGOING, Some(astVariable("b")),
                                                                                     SingleRelationshipPathStep(astVariable("r2"), OUTGOING, Some(astVariable("c")), NilPathStep)))))
    val projection = plans.Projection(filter, Map("result" -> expression))(IdGen)

    val produceResults = plans.ProduceResult(projection, resultColumns)(IdGen)

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
  def executePlan(threadState: PathExpressionThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    visitor.count
  }
}

object PathExpression {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[PathExpression], args:_*)
  }
}

@State(Scope.Thread)
class PathExpressionThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: PathExpression): Unit = {
    val useCompiledExpressions = benchmarkState.PathExpression_engine == CompiledExpressionEngine.NAME
    executablePlan = benchmarkState.buildPlan(EnterpriseInterpreted, useCompiledExpressions)
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
