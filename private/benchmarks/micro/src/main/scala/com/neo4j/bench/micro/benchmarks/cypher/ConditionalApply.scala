/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util
import java.util.Collections

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

@BenchmarkEnabled(true)
class ConditionalApply extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME),
    base = Array(Slotted.NAME, Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("0", "1", "1000"),
    base = Array("1000"))
  @Param(Array[String]())
  var lhsRows: Int = _

  @ParamValues(
    allowed = Array("0.1", "0.5", "0.9"),
    base = Array("0.5"))
  @Param(Array[String]())
  var fractionOfNulls: Double = _

  @ParamValues(
    allowed = Array("0", "1", "1000"),
    base = Array("0", "1000"))
  @Param(Array[String]())
  var rhsRows: Int = _

  override def description = "Conditional Apply"

  private var expectedRowCount: Int = _
  var params: MapValue = _

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    val nullRows: Int = (fractionOfNulls * lhsRows).intValue()
    expectedRowCount = (lhsRows - nullRows) * rhsRows + nullRows
  }

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(rhsRows)
      .isReusableStore(true)
      .build()

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val rhs = "rhs"
    val listType = symbols.CTList(symbols.CTAny)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val unwindVariableName = unwindVariable.name
    val unwind = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariableName, unwindListParameter)(IdGen)

    val rhsAllNodesScan = plans.AllNodesScan("rhs", Set(unwindVariableName))(IdGen)
    val conditionalApply = plans.ConditionalApply(unwind, rhsAllNodesScan, Seq(unwindVariableName))(IdGen)
    val resultColumns = List(unwindVariableName)
    val produceResults = plans.ProduceResult(conditionalApply, columns = resultColumns)(IdGen)

    val table = SemanticTable()
      .addNode(astVariable(rhs))

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: ConditionalApplyThreadState, bh: Blackhole): Long = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(expectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class ConditionalApplyThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: ConditionalApply): Unit = {
    val unwindListValues: java.util.List[String] = new util.ArrayList[String](benchmarkState.lhsRows)
    var i = 0
    while (i < benchmarkState.fractionOfNulls * benchmarkState.lhsRows) {
      unwindListValues.add(i, null.asInstanceOf[String])
      i += 1
    }
    while (i < benchmarkState.lhsRows) {
      unwindListValues.add(i, i.toString)
      i += 1
    }
    Collections.shuffle(unwindListValues)
    val paramsMap = mutable.Map[String, AnyRef]("list" -> unwindListValues).asJava
    benchmarkState.params = ValueUtils.asMapValue(paramsMap)
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object ConditionalApply {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[ConditionalApply])
  }
}





