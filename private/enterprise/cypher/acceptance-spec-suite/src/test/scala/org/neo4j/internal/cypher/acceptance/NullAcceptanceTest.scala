/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class NullAcceptanceTest extends ExecutionEngineFunSuite {

  val anyNull: AnyRef = null.asInstanceOf[AnyRef]
  val expressions = Seq(
    "round(null)",
    "floor(null)",
    "ceil(null)",
    "abs(null)",
    "acos(null)",
    "asin(null)",
    "atan(null)",
    "cos(null)",
    "cot(null)",
    "exp(null)",
    "log(null)",
    "log10(null)",
    "sin(null)",
    "tan(null)",
    "haversin(null)",
    "sqrt(null)",
    "sign(null)",
    "radians(null)",
    "atan2(null, 0.3)",
    "atan2(0.3, null)",
    "null in [1,2,3]",
    "2 in null",
    "null in null",
    "ANY(x in NULL WHERE x = 42)"
  )

  expressions.foreach { expression =>
    test(expression) {
      executeScalar[Any]("RETURN " + expression) should equal(anyNull)
    }
  }
}
