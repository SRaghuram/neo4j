/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import cypher.features.ScenarioTestHelper.printComputedBlacklist
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Disabled
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

class CostMorselAcceptanceTests extends EnterpriseBaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the BaseAcceptanceTest

//  @TestFactory
//  def runCostMorselSingleThreaded(): util.Collection[DynamicTest] = {
//    createTests(scenarios, CostMorselTestConfigSingleThreaded)
//  }
//
//  @TestFactory
//  def runCostMorsel(): util.Collection[DynamicTest] = {
//    createTests(scenarios, CostMorselTestConfig)
//  }

  @Disabled
  def generateBlacklistTCKTestCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostMorselTestConfig, new TestEnterpriseGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
