/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder
import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class MorselSingleThreadedAcceptanceTests extends EnterpriseBaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseAcceptanceTest

  @TestFactory
  def runCostMorselSingleThreaded(): util.Collection[DynamicTest] = {
      createTests(scenarios, CostMorselSingleThreadedTestConfig, () => new TestCommercialDatabaseManagementServiceBuilder())
  }

  @Disabled
  def generateBlacklistCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostMorselSingleThreadedTestConfig, () => new TestCommercialDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
