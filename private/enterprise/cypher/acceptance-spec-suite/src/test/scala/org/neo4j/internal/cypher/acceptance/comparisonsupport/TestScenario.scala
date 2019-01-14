/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Planner => IPDPlanner, PlannerVersion => IPDPlannerVersion, Runtime => IPDRuntime, RuntimeVersion => IPDRuntimeVersion}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.{V2_3, V3_1}
import org.scalatest.Assertions

import scala.util.{Failure, Success, Try}

/**
  * A single scenario, which can be composed to configurations.
  */
case class TestScenario(version: Version, planner: Planner, runtime: Runtime) extends Assertions {

  override def toString: String = name

  def name: String = {
    val versionName = version.name
    val plannerName = planner.preparserOption
    val runtimeName = runtime.preparserOption
    s"$versionName $plannerName $runtimeName"
  }

  def preparserOptions: String = List(version.name, planner.preparserOption, runtime.preparserOption).mkString(" ")

  def checkResultForSuccess(query: String, internalExecutionResult: RewindableExecutionResult): Unit = {
    val ScenarioConfig(reportedRuntime, reportedPlanner, reportedVersion, reportedPlannerVersion) = extractConfiguration(internalExecutionResult)
    if (!runtime.acceptedRuntimeNames.contains(reportedRuntime))
      fail(s"did not use ${runtime.acceptedRuntimeNames} runtime - instead $reportedRuntime was used. Scenario $name")
    if (!planner.acceptedPlannerNames.contains(reportedPlanner))
      fail(s"did not use ${planner.acceptedPlannerNames} planner - instead $reportedPlanner was used. Scenario $name")
    if (!version.acceptedRuntimeVersionNames.contains(reportedVersion))
      fail(s"did not use ${version.acceptedRuntimeVersionNames} runtime version - instead $reportedVersion was used. Scenario $name")
    if (!version.acceptedPlannerVersionNames.contains(reportedPlannerVersion))
      fail(s"did not use ${version.acceptedPlannerVersionNames} planner version - instead $reportedPlannerVersion was used. Scenario $name")
  }

  def checkResultForFailure(query: String, internalExecutionResult: Try[RewindableExecutionResult]): Unit = {
    internalExecutionResult match {
      case Failure(_) => // not unexpected
      case Success(result) =>
        val ScenarioConfig(reportedRuntimeName, reportedPlannerName, reportedVersionName, reportedPlannerVersionName) = extractConfiguration(result)

        if (runtime.acceptedRuntimeNames.contains(reportedRuntimeName)
          && planner.acceptedPlannerNames.contains(reportedPlannerName)
          && version.acceptedRuntimeVersionNames.contains(reportedVersionName)) {
          fail(s"Unexpectedly succeeded using $name for query $query, with $reportedVersionName $reportedRuntimeName runtime and $reportedPlannerVersionName $reportedPlannerName planner.")
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
    val reportedVersion = arguments.collectFirst {
      case IPDRuntimeVersion(reported) => reported
    }
    val reportedPlannerVersion = arguments.collectFirst {
      case IPDPlannerVersion(reported) => reported
    }

    // Neo4j versions 3.2 and earlier do not accurately report when they used procedure runtime/planner,
    // in executionPlanDescription. In those versions, a missing runtime/planner is assumed to mean procedure
    val versionsWithUnreportedProcedureUsage = V2_3 -> V3_1
    val (reportedRuntimeName, reportedPlannerName, reportedVersionName, reportedPlannerVersionName) =
      if (versionsWithUnreportedProcedureUsage.versions.contains(version))
        (reportedRuntime.getOrElse("PROCEDURE"), reportedPlanner.getOrElse("PROCEDURE"), reportedVersion.getOrElse("NONE"), reportedPlannerVersion.getOrElse("NONE"))
      else
        (reportedRuntime.get, reportedPlanner.get, reportedVersion.get, reportedPlannerVersion.get)
    ScenarioConfig(reportedRuntimeName, reportedPlannerName, reportedVersionName, reportedPlannerVersionName)
  }

  def +(other: TestConfiguration): TestConfiguration = other + this
}

case class ScenarioConfig(runtime: String, planner: String, runtimeVersion: String, plannerVersion: String)
