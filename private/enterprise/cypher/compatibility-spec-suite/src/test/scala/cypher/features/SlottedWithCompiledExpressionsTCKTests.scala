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

class SlottedWithCompiledExpressionsTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseTCKTests

  @TestFactory
  def runSlottedWithCompiledExpressions(): util.Collection[DynamicTest] = {
    createTests(scenarios, SlottedWithCompiledExpressionsTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder(), enterpriseDefaultTestConfig)
  }

  @Disabled
  def generateBlacklistSlotted(): Unit = {
    printComputedBlacklist(scenarios, SlottedWithCompiledExpressionsTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
