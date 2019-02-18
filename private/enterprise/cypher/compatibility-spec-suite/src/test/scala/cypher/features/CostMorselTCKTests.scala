/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util
import java.util.Collections

import com.neo4j.test.TestCommercialGraphDatabaseFactory
import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class CostMorselTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostMorsel(): util.Collection[DynamicTest] = {
    if (runOnlySafeScenarios) {
      Collections.emptyList()
    } else {
      createTests(scenarios, CostMorselTestConfig, new TestCommercialGraphDatabaseFactory())
    }
  }

  // REV: is there a common place we could put this, so it's accessible from TCK, Acceptance, & CCS tests?
  private def runOnlySafeScenarios: Boolean = {
    val runExperimental = System.getenv().containsKey("RUN_EXPERIMENTAL")
    !runExperimental
  }

  @Disabled
  def generateBlacklistCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostMorselTestConfig, new TestCommercialGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
