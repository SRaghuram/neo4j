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

class CostSlottedWithCompiledExpressionsAcceptanceTests extends EnterpriseBaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the BaseAcceptanceTest

  @TestFactory
  def runCostSlottedWithCompiledExpressions(): util.Collection[DynamicTest] = {
    createTests(scenarios, CostSlottedWithCompiledExpressionsTestConfig, new TestCommercialGraphDatabaseFactory())
  }

  @Disabled
  def generateBlacklistCostSlotted(): Unit = {
    printComputedBlacklist(scenarios, CostSlottedWithCompiledExpressionsTestConfig, new TestCommercialGraphDatabaseFactory())
    fail("Do not forget to add @Disabled to this method")
  }
}
