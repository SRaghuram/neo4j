/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.opencypher.tools.tck.api.Scenario

class Compatibility31TCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCompatibility31(): util.Collection[DynamicTest] = {
    val filteredScenarios = scenarios.filterNot(testsWithProblems)
    createTests(filteredScenarios, Compatibility31TestConfig, new TestEnterpriseGraphDatabaseFactory())
  }

  //TODO: Fix Schroedinger's test cases in TCK or find way to handle here
  /*
    These tests run with parameters multiple times under the same name.
    The run with the first parameter will succeed and the next ones fail because they try to put e.g. date() inside arrays, which is not possible for
    non-property types in 3.1.
    So they both succeed AND fail -> Thus we cannot "just" blacklist them and need to ignore them completely
   */
  def testsWithProblems(scenario: Scenario): Boolean = {
    (scenario.name.equals("Should store date") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store local time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store local date time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store date time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store duration") && scenario.featureName.equals("TemporalCreateAcceptance"))
  }

  @Disabled
  def generateBlacklistCompatibility31(): Unit = {
    printComputedBlacklist(scenarios, Compatibility31TestConfig, new TestEnterpriseGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
