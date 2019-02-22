/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class TestConfigurationTest extends CypherFunSuite {

  test("should parse empty config") {
    TestConfiguration("") should be(Configs.Empty)
    TestConfiguration(
      """
        |
      """.stripMargin) should be(Configs.Empty)
  }

  test("should parse just planner") {
    TestConfiguration("planner=cost") should be(TestConfiguration(Planners.Cost, Runtimes.all))
  }

  test("should parse just runtime") {
    TestConfiguration("runtime=interpreted") should be(TestConfiguration(Planners.all, Runtimes.Interpreted))
    TestConfiguration("runtime=slotted") should be(TestConfiguration(Planners.all, Runtimes.Slotted))
    TestConfiguration("runtime=slotted expressionEngine=COMPILED") should be(TestConfiguration(Planners.all, Runtimes(Runtimes.Slotted, Runtimes.SlottedWithCompiledExpressions)))
  }

  test("should parse planner and runtime") {
    TestConfiguration("planner=cost runtime=compiled") should be(TestConfiguration(Planners.Cost, Runtimes.CompiledBytecode))
  }

  test("should parse multiple lines") {
    TestConfiguration(
      """planner=cost runtime=compiled
        |runtime=interpreted""".stripMargin
    ) should be(
      TestConfiguration(Planners.Cost, Runtimes.CompiledBytecode) +
      TestConfiguration(Planners.all, Runtimes.Interpreted)
    )
  }
}
