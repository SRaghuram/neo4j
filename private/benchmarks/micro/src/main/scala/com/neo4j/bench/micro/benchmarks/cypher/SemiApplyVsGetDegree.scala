/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.Pos
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.RelationshipDefinition
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class SemiApplyVsGetDegree extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array())
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("SemiApply", "GetDegree"),
    base = Array("SemiApply", "GetDegree")
  )
  @Param(Array[String]())
  var variant: String = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false")
  )
  @Param(Array[String]())
  var dense: Boolean = _

  override def description = "Test SemiApply vs GetDegree performance for exists( (a)-[:REL]->() ) predicates."

  private val NODE_COUNT = 10000
  private val RELATIONSHIP_DEFINITIONS =
    RelationshipDefinition.from("(A:100)")

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withOutRelationships(RELATIONSHIP_DEFINITIONS: _*)
      .withNeo4jConfig(Neo4jConfigBuilder.empty().setDense(dense).build())
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val allNodesScan = plans.AllNodesScan("a", Set.empty)(IdGen)

    val relTypeName = RelTypeName("A")(Pos)
    val filter: LogicalPlan = if (variant == "SemiApply") {
      val argument = plans.Argument(Set("a"))(IdGen)
      val expand = plans.Expand(argument, "a", OUTGOING, Seq(relTypeName), "b", "r", ExpandAll)(IdGen)
      plans.SemiApply(allNodesScan, expand)(IdGen)
    } else {
      plans.Selection(Seq(
        GreaterThan(GetDegree(astVariable("a"), Some(relTypeName), OUTGOING)(Pos), SignedDecimalIntegerLiteral("0")(Pos))(Pos)),
        allNodesScan
      )(IdGen)
    }

    val resultColumns = List("a")
    val produceResults = plans.ProduceResult(filter, resultColumns)(IdGen)

    val table = SemanticTable().
      addNode(astVariable("a")).
      addNode(astVariable("b")).
      addRelationship(astVariable("r"))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: SemiApplyVsGetDegreeThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    subscriber.count
  }
}

@State(Scope.Thread)
class SemiApplyVsGetDegreeThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: SemiApplyVsGetDegree): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object SemiApplyVsGetDegree {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[SemiApplyVsGetDegree], args:_*)
  }
}