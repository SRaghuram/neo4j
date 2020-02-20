/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory

class CompiledTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the EnterpriseBaseTCKTests

  @TestFactory
  def runCostCompiled(): util.Collection[DynamicTest] = {
    // Two scenarios in the tck have the very same name. One fails and one doesn't. Thus we cannot blacklist and must hard ignore until fixed in TCK
    val scenariosWithoutProblematicTCKOne = scenarios.filterNot(s => s.featureName.equals("ListOperations") && s.name.equals("Concatenating lists of same type"))
    createTests(scenariosWithoutProblematicTCKOne, CompiledTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder(), enterpriseDefaultTestConfig)
  }

  @Disabled
  @Test
  def generateBlacklistCostCompiled(): Unit = {
    printComputedBlacklist(scenarios, CompiledTestConfig, () => new TestEnterpriseDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
