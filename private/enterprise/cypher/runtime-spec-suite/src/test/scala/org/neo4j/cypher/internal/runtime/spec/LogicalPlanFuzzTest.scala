/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.values.storable.RandomValues
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class LogicalPlanFuzzTest extends CypherFunSuite
                          with GeneratorDrivenPropertyChecks {

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

  private val managementService = ENTERPRISE.DEFAULT.newGraphManagementService()
  private val graphDb = managementService.database(DEFAULT_DATABASE_NAME)
  private val logProvider: AssertableLogProvider = new AssertableLogProvider()
  private val runtimeTestSupport = new RuntimeTestSupport[EnterpriseRuntimeContext](graphDb, ENTERPRISE.WITH_FUSING(ENTERPRISE.DEFAULT), false, logProvider) with GraphCreation[EnterpriseRuntimeContext] {
    override protected def runtimeTestSupport: RuntimeTestSupport[EnterpriseRuntimeContext] = this
  }
  private val randVals = RandomValues.create()

  private val runtimes = Seq(InterpretedRuntime, SlottedRuntime, PIPELINED/*, PARALLEL*/)

  // This is still an early prototype, thus ignored.
  // It should probably be put into a downstream dependency instead of a regular test, and there are still a number of unsolved problems
  // * How to generate the graph
  // * How to  generate valid plans (or at x % of valid plans) where the invalid ones could be a result of other erros, not system errors
  // Also it only generates very few plans right now.

  ignore("all sorts of queries") {
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

    try {
      forAll(generator) {
        case WithState(logicalQuery, state) =>
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

            results.zipWithIndex.tail.foreach {
              case (Success(result), i) =>
                withClue(s"Comparing ${runtimes(i).name} against ${runtimes.head.name}") {
                  println(s"result.size = ${result.size}")
                  if (result.size <= 1000)
                    result should (contain theSameElementsAs results.head.get)
                  else
                    println(s"skipping check")
                }
              case (Failure(_: CantCompileQueryException), _) => // OK
              case (Failure(e), i) =>
                withClue(s"Error in ${runtimes(i).name}") {
                  throw e
                }
            }
          } finally {
            runtimeTestSupport.stopTx()
          }
      }
    } finally {
      txContext.close()
      tx.close()
      runtimeTestSupport.stop()
      managementService.shutdown()
    }
  }
}
