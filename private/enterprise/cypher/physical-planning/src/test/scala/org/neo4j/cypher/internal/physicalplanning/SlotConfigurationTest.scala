/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlot
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.collection.mutable.ArrayBuffer

class SlotConfigurationTest extends CypherFunSuite with AstConstructionTestSupport {
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
    slots("x") should equal(RefSlot(0, nullable = true, CTList(CTNumber)))
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

  test("foreachSlotOrdered should not choke on LongSlot aliases") {
    // given
    val slots = SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "y" -> LongSlot(1, nullable = false, CTNode)),
      numberOfLongs = 2, numberOfReferences = 0)
    slots.addAlias("z", "x")
    slots.newArgument(Id(0))

    val acc = new SlotAccumulator

    // when
    slots.foreachSlotOrdered(acc.onSlot)

    // then
    acc should haveEvents (Seq(
      OnLongVar("x", LongSlot(0, nullable = false, CTNode)),
      OnLongVar("z", LongSlot(0, nullable = false, CTNode)),
      OnLongVar("y", LongSlot(1, nullable = false, CTNode)),
      OnApplyPlanId(Id(0))
    ), Seq.empty)
  }

  test("foreachSlotOrdered with refs/cached props/longs/applyPlans and skipSlots and aliases") {
    // given
    val slots = SlotConfiguration(Map.empty, 0, 0)
    slots.newArgument(Id(0))
    slots.newLong("a", nullable = false, CTNode)
    slots.addAlias("aa", "a")
    slots.newLong("b", nullable = false, CTNode)
    slots.addAlias("bb", "b")
    slots.addAlias("bbb", "b")
    slots.newArgument(Id(1))

    slots.newReference("c", nullable = false, CTNode)
    slots.newReference("d", nullable = false, CTNode)
    val dCP = CachedProperty("d", varFor("d"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(dCP)
    slots.newReference("e", nullable = false, CTNode)
    slots.addAlias("ee", "e")
    val eCP = CachedProperty("e", varFor("e"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    slots.newCachedProperty(eCP)

    val acc = new SlotAccumulator

    // when
    slots.foreachSlotOrdered(acc.onSlot, skipFirst = SlotConfiguration.Size(nLongs = 2, nReferences = 1))

    // then
    acc should haveEvents (Seq(
      OnLongVar("b", LongSlot(2, nullable = false, CTNode)),
      OnLongVar("bb", LongSlot(2, nullable = false, CTNode)),
      OnLongVar("bbb", LongSlot(2, nullable = false, CTNode)),
      OnApplyPlanId(Id(1))
    ), Seq(
      OnRefVar("d", RefSlot(1, nullable = false, CTNode)),
      OnCachedProp(dCP),
      OnRefVar("e", RefSlot(3, nullable = false, CTNode)),
      OnRefVar("ee", RefSlot(3, nullable = false, CTNode)),
      OnCachedProp(eCP)
    ))
  }

  trait HasSlot {
    def slot: Slot
  }

  sealed trait LongEvent
  case class OnLongVar(string: String, slot: Slot) extends LongEvent with HasSlot
  case class OnApplyPlanId(id: Id) extends LongEvent
  sealed trait RefEvent
  case class OnRefVar(string: String, slot: Slot) extends RefEvent with HasSlot
  case class OnCachedProp(cp: ASTCachedProperty) extends RefEvent


  class SlotAccumulator {
    val longEvents = new ArrayBuffer[LongEvent]()
    val refEvents = new ArrayBuffer[RefEvent]()

    def onSlot(pair: (SlotKey, Slot)): Unit = {
      val (key, slot) = pair
      key match {
        case VariableSlot(name) =>
          if (slot.isLongSlot) longEvents += OnLongVar(name, slot)
          else refEvents += OnRefVar(name, slot)
        case CachedPropertySlot(cp) =>
          refEvents += OnCachedProp(cp)
        case ApplyPlanSlot(id) =>
          longEvents += OnApplyPlanId(id)
      }
    }
  }

  case class haveEvents(expectedLongEvents: Seq[LongEvent], expectedRefEvents: Seq[RefEvent]) extends Matcher[SlotAccumulator] {

    def longEventsComparable(events: Seq[LongEvent]): Seq[AnyRef] = events.collect {
      case OnLongVar(_, slot) => slot
      case o:OnApplyPlanId => o
    }
    def refEventsComparable(events: Seq[RefEvent]): Seq[AnyRef] = events.collect {
      case OnRefVar(_, slot) => slot
      case o:OnCachedProp => o
    }

    def varStringsOf(events: Seq[Any]): Map[Int, Set[Any]] = events.collect {
      case o:HasSlot => o
    }.groupBy(_.slot.offset).mapValues(_.toSet)

    override def apply(actual: SlotAccumulator): MatchResult = {
      val expectedLEC = longEventsComparable(expectedLongEvents)
      val actualLEC = longEventsComparable(actual.longEvents)

      val expectedLongVarStrings = varStringsOf(expectedLongEvents)
      val actualLongVarStrings = varStringsOf(actual.longEvents)

      val expectedREC = refEventsComparable(expectedRefEvents)
      val actualREC = refEventsComparable(actual.refEvents)

      val expectedRefVarStrings = varStringsOf(expectedRefEvents)
      val actualRefVarStrings = varStringsOf(actual.refEvents)

      MatchResult(
        matches = expectedLEC == actualLEC &&
          expectedLongVarStrings == actualLongVarStrings &&
          expectedREC == actualREC &&
          expectedRefVarStrings == actualRefVarStrings,
        rawFailureMessage = s"${actual.longEvents.toList}/${actual.refEvents.toList} were not \n$expectedLongEvents/$expectedRefEvents\n",
        rawNegatedFailureMessage = ""
      )
    }
  }

}
