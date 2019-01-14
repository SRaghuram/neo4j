/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

class Compatibility31AcceptanceTests extends EnterpriseBaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the BaseAcceptanceTest

  @TestFactory
  def runCompatibility31(): util.Collection[DynamicTest] = {
    createTests(scenarios, Compatibility31TestConfig, new TestEnterpriseGraphDatabaseFactory())
  }

  @Disabled
  def generateBlacklistCompatibility31(): Unit = {
    printComputedBlacklist(scenarios, Compatibility31TestConfig, new TestEnterpriseGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
