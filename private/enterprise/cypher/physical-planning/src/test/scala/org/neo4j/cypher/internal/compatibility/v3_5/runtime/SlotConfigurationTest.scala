/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class SlotConfigurationTest extends CypherFunSuite {
  test("allocating same variable name with compatible type but different nullability should increase nullability 1") {
    // given
    val slots = SlotConfiguration.empty
    slots.newLong("x", nullable = false, CTNode)

    // when
    slots.newLong("x", nullable = true, CTNode)

    // then
    slots("x") should equal(LongSlot(0, true, CTNode))
  }

  test("allocating same variable name with compatible type but different nullability should increase nullability 2") {
    // given
    val slots = SlotConfiguration.empty
    slots.newLong("x", nullable = true, CTNode)

    // when
    slots.newLong("x", nullable = false, CTNode)

    // then
    slots("x") should equal(LongSlot(0, true, CTNode))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 1") {
    // given
    val slots = SlotConfiguration.empty
    slots.newReference("x", nullable = false, CTInteger)

    // when
    slots.newReference("x", nullable = false, CTNumber)

    // then
    slots("x") should equal(RefSlot(0, false, CTNumber))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 2") {
    // given
    val slots = SlotConfiguration.empty
    slots.newReference("x", nullable = false, CTAny)

    // when
    slots.newReference("x", nullable = false, CTNumber)

    // then
    slots("x") should equal(RefSlot(0, false, CTAny))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 3") {
    // given
    val slots = SlotConfiguration.empty
    slots.newReference("x", nullable = true, CTMap)

    // when
    slots.newReference("x", nullable = false, CTAny)

    // then
    slots("x") should equal(RefSlot(0, true, CTAny))
  }

  test("allocating same variable name with compatible types should work and get the upper bound type 4") {
    // given
    val slots = SlotConfiguration.empty
    slots.newReference("x", nullable = false, CTList(CTNumber))

    // when
    slots.newReference("x", nullable = true, CTList(CTInteger))

    // then
    slots("x") should equal(RefSlot(0, true, CTList(CTNumber)))
  }

  test("can't overwrite variable name by mistake1") {
    // given
    val slots = SlotConfiguration.empty
    slots.newLong("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newLong("x", nullable = false, CTRelationship))
  }

  test("can't overwrite variable name by mistake2") {
    // given
    val slots = SlotConfiguration.empty
    slots.newLong("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newReference("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake3") {
    // given
    val slots = SlotConfiguration.empty
    slots.newReference("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newLong("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake4") {
    // given
    val slots = SlotConfiguration.empty
    slots.newReference("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](slots.newReference("x", nullable = false, CTRelationship))
  }

  test("copy() creates an immutable copy") {
    // given
    val slots = SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "y" -> LongSlot(1, nullable = false, CTNode)),
      numberOfLongs = 2, numberOfReferences = 0)
    slots.addAlias("z", "x")

    val clone: SlotConfiguration = slots.copy()
    slots should equal(clone)

    // when
    slots.newReference("a", nullable = false, CTNode)
    slots.addAlias("w", "y")

    // then
    slots("x") should equal(LongSlot(0, nullable = false, CTNode))
    slots("y") should equal(LongSlot(1, nullable = false, CTNode))
    slots("a") should equal(RefSlot(0, nullable = false, CTNode))
    slots.isAlias("z") shouldBe true
    slots.isAlias("w") shouldBe true

    clone("x") should equal(LongSlot(0, nullable = false, CTNode))
    clone("y") should equal(LongSlot(1, nullable = false, CTNode))
    clone.get("a") shouldBe empty
    clone.numberOfReferences should equal(0)
    clone.isAlias("z") shouldBe true
    clone.isAlias("w") shouldBe false
    clone("z") should equal(clone("x"))
  }
}
