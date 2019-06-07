/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.{DataGeneratorConfig, DataGeneratorConfigBuilder}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.graphdb.Label
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkEnabled(true)
class ProcedureCall extends AbstractProcedureCall {

  @ParamValues(
    allowed = Array(CompiledByteCode.NAME, CompiledSourceCode.NAME, Interpreted.NAME, EnterpriseInterpreted.NAME, Morsel.NAME),
    base = Array(EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var ProcedureCall_runtime: String = _

  @ParamValues(
    allowed = Array("1", "100", "1000"),
    base = Array("1", "100", "1000"))
  @Param(Array[String]())
  var ProcedureCall_labels: Int = _

  override def description = "procedure call, CALL db.labels yield label, nodeCount RETURN *"

  override protected def getConfig: DataGeneratorConfig = {
    val labels = (0 until ProcedureCall_labels).map(c => Label.label(s"label$c"))
    new DataGeneratorConfigBuilder()
      .withNodeCount(1)
      .withLabels(labels:_*)
      .isReusableStore(true)
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val call = plans.ProcedureCall(plans.Argument()(IdGen), resolvedCall)(IdGen)
    (plans.ProduceResult(call, columns)(IdGen),semanticTable, columns.toList)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ProcedureCallThreadState, bh: Blackhole): Long = {
    val visitor = new CountVisitor(bh)
    threadState.executablePlan.execute(tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(ProcedureCall_labels, visitor)
  }

  override protected def procedureName: QualifiedName = QualifiedName(Seq("db"), "labels")
}

object ProcedureCall {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[ProcedureCall], args:_*)
  }
}

@State(Scope.Thread)
class ProcedureCallThreadState {

  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: ProcedureCall): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.ProcedureCall_runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
