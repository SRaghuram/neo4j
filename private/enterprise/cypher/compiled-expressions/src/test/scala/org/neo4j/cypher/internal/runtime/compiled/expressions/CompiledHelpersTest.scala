/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.Values._

class CompiledHelpersTest extends CypherFunSuite {

  test("assertBooleanOrNoValue") {
    assertBooleanOrNoValue(TRUE) should equal(TRUE)
    assertBooleanOrNoValue(FALSE) should equal(FALSE)
    assertBooleanOrNoValue(NO_VALUE) should equal(NO_VALUE)
    a[CypherTypeException] should be thrownBy assertBooleanOrNoValue(PI)
  }
}
