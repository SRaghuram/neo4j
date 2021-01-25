/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers.assertBooleanOrNoValue
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers.multipleGreaterThanSeek
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers.multipleLessThanSeek
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.FALSE
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.PI
import org.neo4j.values.storable.Values.TRUE
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringValue

class CompiledHelpersTest extends CypherFunSuite {

  test("assertBooleanOrNoValue") {
    assertBooleanOrNoValue(TRUE) should equal(TRUE)
    assertBooleanOrNoValue(FALSE) should equal(FALSE)
    assertBooleanOrNoValue(NO_VALUE) should equal(NO_VALUE)
    a[CypherTypeException] should be thrownBy assertBooleanOrNoValue(PI)
  }

  test("multipleLessThanSeek") {
    multipleLessThanSeek(42,
                         Array(longValue(10), longValue(11), longValue(1)),
                         Array(false, false, true)) should equal(lte(42, 1))
    multipleLessThanSeek(42,
                         Array(longValue(10), longValue(10), longValue(10)),
                         Array(true, false, true)) should equal(lt(42, 10))

    multipleLessThanSeek(42,
                         Array(longValue(10), stringValue("one million"), longValue(10)),
                         Array(true, false, true)) shouldBe null
  }

  test("multipleGreaterThanSeek") {
    multipleGreaterThanSeek(42,
                         Array(longValue(10), longValue(11), longValue(1)),
                         Array(false, true, false)) should equal(gte(42, 11))
    multipleGreaterThanSeek(42,
                         Array(longValue(10), longValue(10), longValue(10)),
                         Array(true, false, true)) should equal(gt(42, 10))

    multipleGreaterThanSeek(42,
                         Array(longValue(10), stringValue("one million"), longValue(10)),
                         Array(true, false, true)) shouldBe null
  }

  private def lt(prop: Int, value: Any): IndexQuery.RangePredicate[_ <: Value] =
    IndexQuery.range(prop, null, false, Values.of(value), false)
  private def lte(prop: Int, value: Any): IndexQuery.RangePredicate[_ <: Value] =
    IndexQuery.range(prop, null, false, Values.of(value), true)
  private def gt(prop: Int, value: Any): IndexQuery.RangePredicate[_ <: Value] =
    IndexQuery.range(prop, Values.of(value), false, null, false)
  private def gte(prop: Int, value: Any): IndexQuery.RangePredicate[_ <: Value] =
    IndexQuery.range(prop, Values.of(value), true, null, false)
}
