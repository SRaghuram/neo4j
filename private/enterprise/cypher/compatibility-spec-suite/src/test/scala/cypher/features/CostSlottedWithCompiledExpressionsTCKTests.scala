/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import java.util

import com.neo4j.test.TestCommercialGraphDatabaseFactory
import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, TestFactory}

class CostSlottedWithCompiledExpressionsTCKTests extends EnterpriseBaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostSlotted(): util.Collection[DynamicTest] = {
    //TODO this is a temporary measure, use CostSlottedWithCompiledExpressionsTestConfig here when TCK is fixed
    createTests(scenarios, new TestConfig(Some("cost-slotted-compiled-expressions.txt"),
                                          "CYPHER planner=cost runtime=slotted expressionEngine=COMPILED"), new TestCommercialGraphDatabaseFactory())
  }

  @Disabled
  def generateBlacklistCostSlotted(): Unit = {
    printComputedBlacklist(scenarios, CostSlottedWithCompiledExpressionsTestConfig, new TestCommercialGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
