/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

class CostMorselTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostMorselSingleThreaded(): util.Collection[DynamicTest] = {
    createTests(scenarios, CostMorselTestConfigSingleThreaded, new TestEnterpriseGraphDatabaseFactory())
  }

  @TestFactory
  def runCostMorsel(): util.Collection[DynamicTest] = {
    createTests(scenarios, CostMorselTestConfig, new TestEnterpriseGraphDatabaseFactory())
  }

  @Disabled
  def generateBlacklistTCKTestCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostMorselTestConfig, new TestEnterpriseGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
