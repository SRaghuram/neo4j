/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.{V3_1, V3_4, v4_0}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class TestConfigurationTest extends CypherFunSuite {

  test("should parse empty config") {
    TestConfiguration("") should be(Configs.Empty)
    TestConfiguration(
      """
        |
      """.stripMargin) should be(Configs.Empty)
  }

  test("should parse just version") {
    TestConfiguration("4.0") should be(TestConfiguration(Versions.v4_0, Planners.all, Runtimes.all))
    TestConfiguration("3.4") should be(TestConfiguration(Versions.V3_4, Planners.all, Runtimes.all))
  }

  test("should parse just planner") {
    TestConfiguration("planner=rule") should be(TestConfiguration(Versions.all, Planners.Rule, Runtimes.all))
    TestConfiguration("planner=cost") should be(TestConfiguration(Versions.all, Planners.Cost, Runtimes.all))
  }

  test("should parse just runtime") {
    TestConfiguration("runtime=interpreted") should be(TestConfiguration(Versions.all, Planners.all, Runtimes.Interpreted))
    TestConfiguration("runtime=slotted") should be(TestConfiguration(Versions.all, Planners.all, Runtimes.Slotted))
    TestConfiguration("runtime=slotted expressionEngine=COMPILED") should be(TestConfiguration(Versions.all, Planners.all, Runtimes(Runtimes.Slotted, Runtimes.SlottedWithCompiledExpressions)))
  }

  test("should parse version and planner") {
    TestConfiguration("4.0 planner=cost") should be(TestConfiguration(Versions.v4_0, Planners.Cost, Runtimes.all))
    TestConfiguration("3.1 planner=rule") should be(TestConfiguration(Versions.V3_1, Planners.Rule, Runtimes.all))
  }

  test("should parse version and planner and runtime") {
    TestConfiguration("4.0 planner=cost runtime=compiled") should be(TestConfiguration(Versions.v4_0, Planners.Cost, Runtimes.CompiledBytecode))
    TestConfiguration("3.1 planner=rule runtime=interpreted") should be(TestConfiguration(Versions.V3_1, Planners.Rule, Runtimes.Interpreted))
  }

  test("should parse multiple lines") {
    TestConfiguration(
      """4.0 planner=cost runtime=compiled
        |3.1 planner=rule runtime=interpreted""".stripMargin) should be(TestConfiguration(Versions.v4_0, Planners.Cost, Runtimes.CompiledBytecode) + TestConfiguration(Versions.V3_1, Planners.Rule, Runtimes.Interpreted))
    TestConfiguration(
      """2.3 planner=rule
        |3.1
        |3.4
        |4.0
      """.stripMargin
    ) should be(
      TestConfiguration(Versions(V3_1, V3_4, v4_0),
        Planners.all,
        Runtimes.all
      ) + Configs.Rule2_3)
  }
}
