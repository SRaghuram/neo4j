/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compiled_runtime

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.{CypherTypeException, ExecutionEngineFunSuite}

class TypeConversionTest extends ExecutionEngineFunSuite {
  test("should not allow adding node and number") {
    val x = createNode()
    val failure = intercept[CypherTypeException] {
      val result = execute("debug=generate_java_source debug=show_java_source profile match (n) return n + {x} as res", "x" -> 5)
      // should not get here, if we do, this is for debugging:
      println(result.executionPlanDescription())
    }

    failure.getMessage should equal("Cannot add `NodeReference` and `Integer`")
  }

  test("shouldHandlePatternMatchingWithParameters") {
    val x = createNode()

    val result = execute("match (x) where x = {startNode} return x", "startNode" -> x)

    result.toList should equal(List(Map("x" -> x)))
  }

  override def execute(q: String, params: (String, Any)*): RewindableExecutionResult =
    super.execute(s"cypher runtime=compiled $q", params:_*)
}
