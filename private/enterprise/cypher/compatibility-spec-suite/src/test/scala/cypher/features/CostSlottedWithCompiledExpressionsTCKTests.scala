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

class CostSlottedWithCompiledExpressionsTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostSlotted(): util.Collection[DynamicTest] = {
    //TODO this is a temporary measure, use CostSlottedWithCompiledExpressionsTestConfig here when TCK is fixed
    createTests(scenarios, new TestConfig(Some("cost-slotted-compiled-expressions.txt"),
                                          "CYPHER planner=cost runtime=slotted expressionEngine=COMPILED"), new TestEnterpriseGraphDatabaseFactory())
  }

  @Disabled
  def generateBlacklistCostSlotted(): Unit = {
    printComputedBlacklist(scenarios, CostSlottedWithCompiledExpressionsTestConfig, new TestEnterpriseGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
