/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.util.Random

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.values.storable.RandomValues
import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import scala.util.Failure
import scala.util.Success
import scala.util.Try

// Class name not ending in `Test` to make sure `mvn test` won't run it
class LogicalPlanFuzzTesting extends CypherFunSuite {

  private val managementService = ENTERPRISE.DEFAULT.newGraphManagementService()
  private val graphDb = managementService.database(DEFAULT_DATABASE_NAME)
  private val logProvider: AssertableLogProvider = new AssertableLogProvider()
  private val runtimeTestSupport = new RuntimeTestSupport[EnterpriseRuntimeContext](graphDb, ENTERPRISE.WITH_FUSING(ENTERPRISE.DEFAULT), false, logProvider) with GraphCreation[EnterpriseRuntimeContext] {
    override protected def runtimeTestSupport: RuntimeTestSupport[EnterpriseRuntimeContext] = this
  }

  private val runtimes = Seq(InterpretedRuntime, SlottedRuntime, PIPELINED/*, PARALLEL*/)

  // This is still an early prototype, and there are a number of unsolved problems
  // * How to generate the graph
  // * How to  generate valid plans (or at x % of valid plans) where the invalid ones could be a result of other erros, not system errors
  // Also it only generates very few plans right now.

  test("all sorts of queries") {
    // Create the data (all executors use the same database instance)
    runtimeTestSupport.start()
    runtimeTestSupport.startTx()
    runtimeTestSupport.bidirectionalBipartiteGraph(10, "A", "B", "AB", "BA")
    runtimeTestSupport.nodeGraph(10, "A")
    runtimeTestSupport.nodeGraph(12, "B")
    runtimeTestSupport.nodeGraph(5, "C")
    runtimeTestSupport.tx.commit()
    runtimeTestSupport.stopTx()

    runtimeTestSupport.startTx()
    val tx = runtimeTestSupport.tx
    val txContext = runtimeTestSupport.txContext

    val generator = LogicalQueryGenerator.logicalQuery(txContext, Cost(10000))

    val initialSeed = Seed.random() // use `Seed.fromBase64` to reproduce test failures
    val iterationCount = sys.env.getOrElse("LOGICAL_PLAN_FUZZ_ITERATIONS", "100").toInt

    try {
      Range(0, iterationCount).foldLeft(initialSeed) {
        (seed: Seed, iter: Int) =>
          println()
          println(s"[${iter + 1}/$iterationCount] seed = ${seed.toBase64}")
          runTest(seed, generator)
          Seed.random()
      }
    } finally {
      txContext.close()
      tx.close()
      runtimeTestSupport.stop()
      managementService.shutdown()
    }
  }

  private def runTest(seed: Seed, generator: Gen[WithState[LogicalQuery]]): Unit = {
    val WithState(logicalQuery, state) = generator.pureApply(Gen.Parameters.default, seed)
    val randVals = RandomValues.create(new Random(seed.long._1))
    val parameters = state.parameters.map(_ -> randVals.nextValue().asObject()).toMap

    val cost = CardinalityCostModel(logicalQuery.logicalPlan, QueryGraphSolverInput.empty, logicalQuery.cardinalities)

    println(logicalQuery.logicalPlan)
    println(parameters)
    println(cost)

    runtimeTestSupport.startTx()

    try {
      val results = runtimes.map {
        runtime => Try(runtimeTestSupport.executeAndConsumeTransactionally(logicalQuery, runtime, parameters))
      }

      results.zip(runtimes).foreach {
        case (Failure(_: CantCompileQueryException), _) => // OK
        case (Failure(_: CypherTypeException | _: ParameterWrongTypeException), _) => // Ignore these for now, need to generate parameters of appropriate types first
        case (Failure(e), runtime) =>
          withClue(s"Error in ${runtime.name}") {
            fail(e)
          }
        case (Success(_), _) => // checking `Failure`s first
      }

      val referenceResult = results.head

      results.zip(runtimes).tail.foreach {
        case (Success(result), runtime) if referenceResult.isSuccess =>
          withClue(s"Comparing ${runtime.name} against ${runtimes.head.name}") {
            println(s"result.size = ${result.size}")
            if (result.size <= 1000)
              result should (contain theSameElementsAs referenceResult.get)
            else
              println(s"skipping check")
          }
        case (Success(_), runtime) if referenceResult.isFailure =>
          fail(s"Failed in ${runtimes.head.name}, but succeded in ${runtime.name}", referenceResult.failed.get)
        case (Failure(_), _) => // already checked
      }
    } finally {
      runtimeTestSupport.stopTx()
    }
  }
}
