/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util
import java.util.Collections

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder
import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class ParallelRuntimeTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseTCKTests

  @TestFactory
  def runCostMorsel(): util.Collection[DynamicTest] = {
    if (runOnlySafeScenarios) {
      Collections.emptyList()
    } else {
      createTests(scenarios, CostParallelTestConfig, () => new TestCommercialDatabaseManagementServiceBuilder())
    }
  }

  private def runOnlySafeScenarios: Boolean = {
    val runExperimental = System.getenv().containsKey("RUN_EXPERIMENTAL")
    !runExperimental
  }

  @Disabled
  def generateBlacklistCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostParallelTestConfig, () => new TestCommercialDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
