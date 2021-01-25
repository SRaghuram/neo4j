/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

/**
  * A set of scenarios.
  */
case class TestConfiguration(scenarios: Set[TestScenario]) {

  def +(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios ++ other.scenarios)

  def -(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios -- other.scenarios)

  def /\(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios intersect other.scenarios)

  def containsScenario(scenario: TestScenario): Boolean = this.scenarios.contains(scenario)
}

object TestConfiguration {
  implicit def scenarioToTestConfiguration(scenario: TestScenario): TestConfiguration = TestConfiguration(scenario)

  def apply(scenarios: TestScenario*): TestConfiguration = {
    TestConfiguration(scenarios.toSet)
  }

  def apply(planners: Planners, runtimes: Runtimes): TestConfiguration = {
    val scenarios = for (p <- planners.planners;
                         r <- runtimes.runtimes)
      yield TestScenario(p, r)
    TestConfiguration(scenarios.toSet)
  }

  def apply(description:String): TestConfiguration = {
    // Splitting on "\n" works in both Unix and Windows, since the Strings that come in here are from our source code, which has unix line endings
    val configs = description.split("\n").map(_.trim)
    configs.map { stringDescription =>
      if(stringDescription.isEmpty) {
        empty
      } else {
        val args = stringDescription.split(" ")
        val planners = Planners.definedBy(args)
        val runtimes = Runtimes.definedBy(args)
        TestConfiguration(planners, runtimes)
      }
    }.reduceOption(_ + _).getOrElse(Configs.Empty)
  }

  def empty: TestConfiguration = {
    TestConfiguration(Nil: _*)
  }
}
