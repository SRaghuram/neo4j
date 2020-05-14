/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values.stringValue

class SlottedRowTest extends CypherFunSuite {

  private def slots(longs: Int, refs: Int) = {
    val sc = SlotConfiguration.empty
    for(i <- 1 to longs) sc.newLong(s"long$i", nullable = false, CTNode)
    for(i <- 1 to refs) sc.newReference(s"ref$i", nullable = true, CTAny)
    sc
  }

  test("copy fills upp the first few elements") {
    val input = SlottedRow(slots(2, 1))
    val result = SlottedRow(slots(3, 2))

    input.setLongAt(0, 42)
    input.setLongAt(1, 666)
    input.setRefAt(0, stringValue("21"))

    result.copyFrom(input, 2, 1)

    result.getLongAt(0) should equal(42)
    result.getLongAt(1) should equal(666)
    result.getRefAt(0) should equal(stringValue("21"))
  }

  test("copy fails if copy from larger") {
    val input = SlottedRow(slots(4, 0))
    val result = SlottedRow(slots(2, 0))

    intercept[InternalException](result.copyFrom(input, 4, 0))
  }

  test("copy fails if copy from larger 2") {
    val input = SlottedRow(slots(0, 4))
    val result = SlottedRow(slots(0, 2))

    intercept[InternalException](result.copyFrom(input, 0, 4))
  }

  test("can merge nullable RefSlots which are null") {
    val leftSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlotConfigurationUtils.generateSlotAccessorFunctions(leftSlots)
    val rightSlots = slots(0, 0).newReference("a", nullable = true, CTAny)
    SlottedRow(leftSlots).mergeWith(SlottedRow(rightSlots), null) // should not fail
  }

  test("mergeWith - cached properties on rhs only") {
    // given
    val slots =
      SlotConfiguration.empty
        .newCachedProperty(prop("n", "name"))
        .newCachedProperty(prop("n", "extra cached"))

    val extraCachedOffset = offsetFor(prop("n", "extra cached"), slots)

    val lhsCtx = SlottedRow(slots)

    val rhsCtx = SlottedRow(slots)
    rhsCtx.setCachedProperty(prop("n", "name"), stringValue("b"))

    // when
    lhsCtx.mergeWith(rhsCtx, null)

    // then
    def cachedPropAt(key: CachedProperty, ctx: CypherRow) =
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

    val result = SlottedRow(resultSlots)
    result.setCachedProperty(prop("a", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "name"), stringValue("initial"))
    result.setCachedProperty(prop("b", "age"), stringValue("initial"))

    val argSlots =
      SlotConfiguration.empty
        .newCachedProperty(prop("b", "name"))
        .newCachedProperty(prop("c", "name"))
        .newCachedProperty(prop("c", "age"))

    val arg = SlottedRow(argSlots)
    arg.setCachedProperty(prop("b", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "name"), stringValue("arg"))
    arg.setCachedProperty(prop("c", "age"), stringValue("arg"))

    // when
    result.mergeWith(arg, null)

    // then
    def cachedPropAt(key: CachedProperty) =
      result.getCachedPropertyAt(resultSlots.getCachedPropertyOffsetFor(key))

    cachedPropAt(prop("a", "name")) should be(stringValue("initial"))
    cachedPropAt(prop("b", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("b", "age")) should be(stringValue("initial"))
    cachedPropAt(prop("c", "name")) should be(stringValue("arg"))
    cachedPropAt(prop("c", "age")) should be(stringValue("arg"))
  }

  private def prop(node: String, prop: String) =
    CachedProperty(node, Variable(node)(InputPosition.NONE), PropertyKeyName(prop)(InputPosition.NONE), NODE_TYPE)(InputPosition.NONE)

  private def mutatingLeftDoesNotAffectRight(left: CypherRow, right: CypherRow, extraCachedOffset: Int): Unit = {
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

  private def offsetFor(key: CachedProperty, slots: SlotConfiguration) = slots.getCachedPropertyOffsetFor(key)
}
