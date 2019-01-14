/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlotConfigurationUtils}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.logical.plans.CachedNodeProperty
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.cypher.internal.v4_0.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, InternalException}

class SlottedExecutionContextTest extends CypherFunSuite {

  private def slots(longs: Int, refs: Int) = SlotConfiguration(Map.empty, longs, refs)

  test("copy fills upp the first few elements") {
    val input = SlottedExecutionContext(slots(2, 1))
    val result = SlottedExecutionContext(slots(3, 2))

    input.setLongAt(0, 42)
    input.setLongAt(1, 666)
    input.setRefAt(0, stringValue("21"))

    result.copyFrom(input, 2, 1)

    result.getLongAt(0) should equal(42)
    result.getLongAt(1) should equal(666)
    result.getRefAt(0) should equal(stringValue("21"))
  }

  test("copy fails if copy from larger") {
    val input = SlottedExecutionContext(slots(4, 0))
    val result = SlottedExecutionContext(slots(2, 0))

    intercept[InternalException](result.copyFrom(input, 4, 0))
  }

  test("copy fails if copy from larger 2") {
    val input = SlottedExecutionContext(slots(0, 4))
    val result = SlottedExecutionContext(slots(0, 2))

    intercept[InternalException](result.copyFrom(input, 0, 4))
  }

  test("can merge nullable RefSlots which are null") {
    val leftSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlotConfigurationUtils.generateSlotAccessorFunctions(leftSlots)
    val rightSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlottedExecutionContext(leftSlots).mergeWith(SlottedExecutionContext(rightSlots)) // should not fail
  }

  test("mergeWith - cached properties on rhs only") {
    // given
    val slots =
      SlotConfiguration.empty
      .newCachedProperty(prop("n", "name"))
      .newCachedProperty(prop("n", "extra cached"))

    val extraCachedOffset = offsetFor(prop("n", "extra cached"), slots)

    val lhsCtx = SlottedExecutionContext(slots)

    val rhsCtx = SlottedExecutionContext(slots)
    rhsCtx.setCachedProperty(prop("n", "name"), stringValue("b"))

    // when
    lhsCtx.mergeWith(rhsCtx)

    // then
    def cachedPropAt(key: CachedNodeProperty, ctx: ExecutionContext) =
      ctx.getCachedPropertyAt(offsetFor(key, slots))

    cachedPropAt(prop("n", "name"), lhsCtx) should be(stringValue("b"))
    cachedPropAt(prop("n", "name"), rhsCtx) should be(stringValue("b"))

    mutatingLeftDoesNotAffectRight(rhsCtx, lhsCtx, extraCachedOffset)
  }

  test("mergeWith() includes cached node properties") {
    // given
    val resultSlots =
      SlotConfiguration.empty
        .newCachedProperty(prop("a", "name"))
        .newCachedProperty(prop("b", "name"))
        .newCachedProperty(prop("b", "age"))
        .newCachedProperty(prop("c", "name"))
        .newCachedProperty(prop("c", "age"))

    val result = SlottedExecutionContext(resultSlots)
    result.setCachedProperty(prop("a", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "age"), stringValue("initial"))

    val argSlots =
      SlotConfiguration.empty
        .newCachedProperty(prop("b", "name"))
        .newCachedProperty(prop("c", "name"))
        .newCachedProperty(prop("c", "age"))

    val arg = SlottedExecutionContext(argSlots)
    arg.setCachedProperty(prop("b", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "age"), stringValue("arg"))

    // when
    result.mergeWith(arg)

    // then
    def cachedPropAt(key: CachedNodeProperty) =
      result.getCachedPropertyAt(resultSlots.getCachedNodePropertyOffsetFor(key))

    cachedPropAt(prop("a", "name")) should be(stringValue("initial"))
    cachedPropAt(prop("b", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("b", "age")) should be(stringValue("initial"))
    cachedPropAt(prop("c", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("c", "age")) should be(stringValue("arg"))
  }

  private def prop(node: String, prop: String) =
    CachedNodeProperty(node, PropertyKeyName(prop)(InputPosition.NONE))(InputPosition.NONE)

  private def mutatingLeftDoesNotAffectRight(left: ExecutionContext, right: ExecutionContext, extraCachedOffset: Int): Unit = {
    // given
    left should not be theSameInstanceAs(right)
    left.getCachedPropertyAt(extraCachedOffset) should equal(null)
    right.getCachedPropertyAt(extraCachedOffset) should equal(null)

    // when (left is modified)
    left.setCachedPropertyAt(extraCachedOffset, BooleanValue.FALSE)

    // then (only left should be modified)
    left.getCachedPropertyAt(extraCachedOffset) should equal(BooleanValue.FALSE)
    right.getCachedPropertyAt(extraCachedOffset) should equal(null)
  }

  private def offsetFor(key: CachedNodeProperty, slots: SlotConfiguration) = slots.getCachedNodePropertyOffsetFor(key)
}
