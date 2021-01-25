/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import cypher.features.ScenarioTestHelper.createTests
import cypher.features.ScenarioTestHelper.printComputedBlacklist
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory

class PipelinedFullTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseTCKTests

  @TestFactory
  def runPipelinedFull(): util.Collection[DynamicTest] = {
    createTests(scenarios, PipelinedFullTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder(), enterpriseDefaultTestConfig)
  }

  @Disabled
  def generateBlacklistPipelinedFull(): Unit = {
    printComputedBlacklist(scenarios, PipelinedFullTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
