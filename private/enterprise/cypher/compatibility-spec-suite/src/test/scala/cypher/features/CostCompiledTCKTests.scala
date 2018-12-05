/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import com.neo4j.test.TestEnterpriseGraphDatabaseFactory
import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class CostCompiledTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostCompiled(): util.Collection[DynamicTest] = {
    createTests(scenarios, CostCompiledTestConfig, new TestEnterpriseGraphDatabaseFactory())
  }

  @Disabled
  def generateBlacklistCostCompiled(): Unit = {
    printComputedBlacklist(scenarios, CostCompiledTestConfig, new TestEnterpriseGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
