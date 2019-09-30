/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class MorselSingleThreadedFullTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseTCKTests

  @TestFactory
  def runCostMorselSingleThreadedFull(): util.Collection[DynamicTest] = {
    createTests(scenarios, CostMorselSingleThreadedFullTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder())
  }

  @Disabled
  def generateBlacklistCostMorselFull(): Unit = {
    printComputedBlacklist(scenarios, CostMorselSingleThreadedFullTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
