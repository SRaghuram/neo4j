/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util
import java.util.Collections

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import cypher.features.ScenarioTestHelper.createTests
import cypher.features.ScenarioTestHelper.printComputedBlacklist
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs

class ParallelRuntimeAcceptanceTests extends EnterpriseBaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseAcceptanceTest

  @TestFactory
  def runParallel(): util.Collection[DynamicTest] = {
    if (Configs.runOnlySafeScenarios) {
      Collections.emptyList()
    } else {
      createTests(scenarios, ParallelTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder(), enterpriseDefaultTestConfig + (GraphDatabaseInternalSettings.cypher_worker_count -> java.lang.Integer.valueOf(0)))
    }
  }

  @Disabled
  def generateBlacklistCostParallel(): Unit = {
    printComputedBlacklist(scenarios, ParallelTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
