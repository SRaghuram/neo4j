/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import cypher.features.{Phase, ScenarioTestHelper}
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.{Planner => IPDPlanner, Runtime => IPDRuntime}
import org.scalatest.Assertions

import scala.util.{Failure, Success, Try}

/**
  * A single scenario, which can be composed to configurations.
  */
case class TestScenario(planner: Planner, runtime: Runtime) extends Assertions {

  override def toString: String = name

  def name: String = {
    val plannerName = planner.preparserOption
    val runtimeName = runtime.preparserOption
    s"$plannerName $runtimeName"
  }

  def preparserOptions: String = List(planner.preparserOption, runtime.preparserOption).mkString(" ")

  def checkResultForSuccess(query: String, internalExecutionResult: RewindableExecutionResult, silentUnexpectedSuccess: Boolean): Unit = {
    val ScenarioConfig(reportedRuntime, reportedPlanner) = extractConfiguration(internalExecutionResult)
    if (!silentUnexpectedSuccess) {
      if (!runtime.acceptedRuntimeNames.contains(reportedRuntime))
        fail(s"did not use ${runtime.acceptedRuntimeNames} runtime - instead $reportedRuntime was used. Scenario $name")
      if (!planner.acceptedPlannerNames.contains(reportedPlanner))
        fail(s"did not use ${planner.acceptedPlannerNames} planner - instead $reportedPlanner was used. Scenario $name")
    }
  }

  def checkResultForFailure(query: String, internalExecutionResult: Try[RewindableExecutionResult], maybePhase: Option[String], silentUnexpectedSuccess: Boolean): Unit = {
    internalExecutionResult match {
      case Failure(_) if maybePhase.contains(Phase.compile) =>
        // A compile-time failure is expected and ok
      case Failure(_) if maybePhase.isEmpty =>
        // Not executed is also expected and ok
      case Failure(e) =>
        val phase = maybePhase.get
        fail(s"""Failed at $phase using $name for query:
                |
                |$query
                |
                |(NOTE: This test is marked as expected to fail, but failing at $phase is not ok)
                |""".stripMargin, e)
      case Success(result) =>
        val ScenarioConfig(reportedRuntimeName, reportedPlannerName) = extractConfiguration(result)

        if (runtime.acceptedRuntimeNames.contains(reportedRuntimeName)
          && planner.acceptedPlannerNames.contains(reportedPlannerName)) {

          if (!silentUnexpectedSuccess) {
            fail(s"""Unexpectedly succeeded using $name for query:
                    |
                    |$query
                    |
                    |(Actually executed with $reportedRuntimeName runtime and $reportedPlannerName planner)
                    |""".stripMargin)
          } else {
            ScenarioTestHelper.unexpectedSuccessCount.getAndAdd(1)
          }
        }
    }
  }

  private def extractConfiguration(result: RewindableExecutionResult): ScenarioConfig =
    extractConfiguration(result.executionPlanDescription().arguments)

  private def extractConfiguration(arguments: Seq[Argument]): ScenarioConfig = {
    val reportedRuntime = arguments.collectFirst {
      case IPDRuntime(reported) => reported
    }
    val reportedPlanner = arguments.collectFirst {
      case IPDPlanner(reported) => reported
    }
    ScenarioConfig(reportedRuntime.get, reportedPlanner.get)
  }

  def +(other: TestConfiguration): TestConfiguration = other + this
}

case class ScenarioConfig(runtime: String, planner: String)
