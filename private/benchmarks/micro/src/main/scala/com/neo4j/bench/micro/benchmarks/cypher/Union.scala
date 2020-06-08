/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
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
class Union extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1000", "100000", "1000000"),
    base = Array("100000"))
  @Param(Array[String]())
  var rows: Int = _

  @ParamValues(
    allowed = Array("1", "6", "36"),
    base = Array("36"))
  @Param(Array[String]())
  var columns: Int = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true", "false"))
  @Param(Array[String]())
  var sameSlots: Boolean = _

  override def description = "Union"

  private var expectedRowCount: Int = _

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(rows)
      .isReusableStore(true)
      .build()

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    expectedRowCount = rows + rows
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val a = "a"
    val projectedNames = (0 until columns).map(i => s"a_$i").toList
    val projectionsAsNodes = projectedNames.map(name => name -> astVariable(a)).toMap

    val lhs = {
      val allNodesScanLHS = plans.AllNodesScan(a, Set.empty)(IdGen)
      // This projection will use aliases in the LHS and RHS pipes, but currently the outgoing pipe will allocate one slot for each alias.
      // If we ever implement a less conservative approach to aliases in Union and want to continue benchmarking the worst case, we will
      // have to revisit this.
      plans.Projection(allNodesScanLHS, projectionsAsNodes)(IdGen)
    }

    val rhs = if (sameSlots) {
      val allNodesScanRHS = plans.AllNodesScan(a, Set.empty)(IdGen)
      plans.Projection(allNodesScanRHS, projectionsAsNodes)(IdGen)
    } else {
      // We keep the ANS, even if we don't use it, to be able to compare better with `sameSlots=true`
      val allNodesScanRHS = plans.AllNodesScan(a, Set.empty)(IdGen)
      val projectionsWithAnyRefs = projectedNames.map(name => name -> astLiteralFor(name, STR_SML)).toMap
      plans.Projection(allNodesScanRHS, projectionsWithAnyRefs)(IdGen)
    }

    val union = plans.Union(lhs, rhs)(IdGen)
    val produceResults = plans.ProduceResult(union, columns = projectedNames)(IdGen)

    val table = (a +: projectedNames).foldLeft(SemanticTable()) {
      case (t, name) => t.addVariable(astVariable(name))
    }

    (produceResults, table, projectedNames)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: UnionThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(expectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class UnionThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: Union): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
