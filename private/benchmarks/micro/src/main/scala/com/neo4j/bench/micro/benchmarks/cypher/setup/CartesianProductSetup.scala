/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.setup

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.cypher.AbstractCypherBenchmark
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.benchmarks.cypher.ExecutablePlan
import com.neo4j.bench.micro.benchmarks.cypher.Parallel
import com.neo4j.bench.micro.benchmarks.cypher.Pipelined
import com.neo4j.bench.micro.benchmarks.cypher.TestSetup
import com.neo4j.bench.micro.benchmarks.cypher.plan.builder.BenchmarkSetupPlanBuilder
import org.neo4j.cypher.internal.PipelinedRuntimeResult
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
class CartesianProductSetup extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Pipelined.NAME, Parallel.NAME),
    base = Array(Pipelined.NAME))
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("100"),
    base = Array("100"))
  @Param(Array[String]())
  var depth: Int = _

  override def description = "CartesianProduct setup"

  override def setup(planContext: PlanContext): TestSetup = {
    val initialPlanBuilder = new BenchmarkSetupPlanBuilder()
      .produceResults("n")

    val planBuilderWithCartesianProduct = (1 to depth).foldLeft(initialPlanBuilder) { (accumulatingBuilder, number) =>
      accumulatingBuilder
        .cartesianProduct()
        .|.allNodeScan(s"m$number")
    }

    val completePlanBuilder = planBuilderWithCartesianProduct.allNodeScan("n")

    completePlanBuilder.build()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: CartesianProductSetupThreadState, bh: Blackhole): Int = {
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(tx = threadState.tx, subscriber = subscriber)

    // This is what we're interested in measuring
    val runtimeResult = result.asInstanceOf[PipelinedRuntimeResult].createQuerySubscription

    // This is here to avoid dead code elimination
    if (runtimeResult != null) {
      1
    } else {
      0
    }
  }
}

@State(Scope.Thread)
class CartesianProductSetupThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _

  @Setup
  def setUp(benchmarkState: CartesianProductSetup): Unit = {
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
    tx = benchmarkState.beginInternalTransaction()
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}

object CartesianProductSetup {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[CartesianProductSetup])
  }
}