/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

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
    TestConfiguration("runtime=slotted expressionEngine=interpreted") should be(TestConfiguration(Planners.all, Runtimes.SlottedWithInterpretedExpressions))
    TestConfiguration("runtime=slotted expressionEngine=compiled") should be(TestConfiguration(Planners.all, Runtimes.SlottedWithCompiledExpressions))
  }

  test("should parse planner and runtime") {
    TestConfiguration("planner=cost runtime=interpreted") should be(TestConfiguration(Planners.Cost, Runtimes.Interpreted))
  }

  test("should parse multiple lines") {
    TestConfiguration(
      """planner=cost runtime=parallel
        |runtime=interpreted""".stripMargin
    ) should be(
      TestConfiguration(Planners.Cost, Runtimes.Parallel) +
      TestConfiguration(Planners.all, Runtimes.Interpreted)
    )
  }
}
