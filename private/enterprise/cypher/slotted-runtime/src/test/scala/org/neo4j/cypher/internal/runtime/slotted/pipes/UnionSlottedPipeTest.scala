/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.computeUnionMapping
import org.neo4j.cypher.internal.runtime.{NodeOperations, QueryContext, RelationshipOperations}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeEntity, fromRelationshipEntity}
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{longValue, stringArray, stringValue}
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.{NodeValue, RelationshipValue, VirtualValues}

class UnionSlottedPipeTest extends CypherFunSuite with AstConstructionTestSupport {

  private def union(lhsSlots: SlotConfiguration,
                    rhsSlots: SlotConfiguration,
                    out: SlotConfiguration,
                    lhsData: Iterable[Map[Any, Any]],
                    rhsData: Iterable[Map[Any, Any]]) = {
    val lhs = FakeSlottedPipe(lhsData, lhsSlots)
    val rhs = FakeSlottedPipe(rhsData, rhsSlots)
    val union = UnionSlottedPipe(lhs, rhs, out, computeUnionMapping(lhsSlots, out), computeUnionMapping(rhsSlots, out) )()
    val context = mock[QueryContext]
    val nodeOps = mock[NodeOperations]
    when(nodeOps.getById(any())).thenAnswer(new Answer[NodeValue] {
      override def answer(invocation: InvocationOnMock): NodeValue =
        fromNodeEntity(newMockedNode(invocation.getArgument[Long](0)))
    })
    when(context.nodeOps).thenReturn(nodeOps)
    val relOps = mock[RelationshipOperations]
    when(relOps.getById(any())).thenAnswer(new Answer[RelationshipValue] {
      override def answer(invocation: InvocationOnMock): RelationshipValue =
        fromRelationshipEntity(newMockedRelationship(invocation.getArgument[Long](0)))
    })
    when(context.relationshipOps).thenReturn(relOps)
    val res = union.createResults(QueryStateHelper.emptyWith(query = context)).toList.map {
      case e: SlottedExecutionContext =>
        e.slots.mapSlot(
          onVariable = {
          case (k, s: LongSlot) =>
            val value = if (s.typ == CTNode) nodeValue(e.getLongAt(s.offset))
            else if (s.typ == CTRelationship) relValue(e.getLongAt(s.offset))
            else throw new AssertionError("This is clearly not right")
            k -> value
          case (k, s) => k -> e.getRefAt(s.offset)
        }, onCachedProperty = {
            case (cachedProp, refSlot) => cachedProp.asCanonicalStringVal -> e.getCachedPropertyAt(refSlot.offset)
          }).toMap
    }

    res
  }

  test("should handle references") {
    // Given
    val lhsSlots = SlotConfiguration.empty.newReference("x", nullable = false, CTAny)
    val rhsSlots = SlotConfiguration.empty.newReference("x", nullable = false, CTAny)
    val out = SlotConfiguration.empty.newReference("x", nullable = false, CTAny)
    val lhsData = List(Map[Any, Any]("x" -> 42))
    val rhsData = List(Map[Any, Any]("x" -> 43))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> longValue(42)), Map("x" -> longValue(43))))
  }

  test("should handle mixed longslot and refslot") {
    // Given
    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val rhsSlots = SlotConfiguration.empty.newReference("x", nullable = false, CTAny)
    val out = SlotConfiguration.empty.newReference("x", nullable = false, CTAny)
    val lhsData = List(Map[Any, Any]("x" -> 42))
    val rhsData = List(Map[Any, Any]("x" -> 43))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> nodeValue(42)), Map("x" -> longValue(43))))
  }

  test("should handle two node longslots") {
    // Given
    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val rhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val out = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val lhsData = List(Map[Any, Any]("x" -> 42))
    val rhsData = List(Map[Any, Any]("x" -> 43))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)
    // Then
    result should equal(
      List(Map("x" -> nodeValue(42)), Map("x" -> nodeValue(43))))
  }

  test("should handle cached properties") {
    // Given
    val cachedProp = cachedNodeProp("x", "prop")
    val onlyLeftCached = cachedNodeProp("x", "foo")
    val onlyRightCached = cachedNodeProp("x", "bar")
    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode).newCachedProperty(cachedProp).newCachedProperty(onlyLeftCached)
    val rhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode).newCachedProperty(cachedProp).newCachedProperty(onlyRightCached)
    val out = SlotConfiguration.empty.newLong("x", nullable = false, CTNode).newCachedProperty(cachedProp)
    val lhsData = List(Map[Any, Any]("x" -> 42, cachedProp -> "cached prop left", onlyLeftCached -> "cached foo"))
    val rhsData = List(Map[Any, Any]("x" -> 43, cachedProp -> "cached prop right", onlyRightCached -> "cached bar"))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)
    // Then
    result should equal(
      List(Map("x" -> nodeValue(42), "cache[x.prop]" -> Values.stringValue("cached prop left")),
        Map("x" -> nodeValue(43), "cache[x.prop]" -> Values.stringValue("cached prop right"))))
  }

  test("should handle two relationship longslots") {
    // Given
    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTRelationship)
    val rhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTRelationship)
    val out = SlotConfiguration.empty.newLong("x", nullable = false, CTRelationship)
    val lhsData = List(Map[Any, Any]("x" -> 42))
    val rhsData = List(Map[Any, Any]("x" -> 43))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> relValue(42)), Map("x" -> relValue(43))))
  }

  test("should handle one long slot and one relationship slot") {
    // Given
    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val rhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTRelationship)
    val out = SlotConfiguration.empty.newReference("x", nullable = false, CTAny)
    val lhsData = List(Map[Any, Any]("x" -> 42))
    val rhsData = List(Map[Any, Any]("x" -> 43))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> nodeValue(42)), Map("x" -> relValue(43))))
  }

  test("should handle multiple columns") {
    // Given
    val lhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val rhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTRelationship)
      .newLong("z", nullable = false, CTRelationship)
    val out = SlotConfiguration.empty
      .newReference("x", nullable = false, CTAny)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val lhsData: Seq[Map[Any, Any]] = List(Map("x" -> 42, "y" -> 1337, "z" -> "FOO"))
    val rhsData: Seq[Map[Any, Int]] = List(Map("x" -> 43, "y" -> 44, "z" -> 45))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)

    // Then
    result should equal(
      List(
        Map("x" -> nodeValue(42), "y" -> relValue(1337),"z" -> stringValue("FOO")),
        Map("x" -> relValue(43), "y" -> relValue(44),"z" -> relValue(45))
      ))
  }

  test("should handle multiple columns in permutated order") {
    // Given
    val lhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val rhsSlots = SlotConfiguration.empty
      .newLong("y", nullable = false, CTRelationship)
      .newLong("z", nullable = false, CTRelationship)
      .newLong("x", nullable = false, CTRelationship)
    val out = SlotConfiguration.empty
      .newReference("x", nullable = false, CTAny)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val lhsData: Seq[Map[Any, Any]] = List(Map("x" -> 42, "y" -> 1337, "z" -> "FOO"))
    val rhsData: Seq[Map[Any, Int]] = List(Map("x" -> 43, "y" -> 44, "z" -> 45))

    // When
    val result = union(lhsSlots, rhsSlots, out, lhsData, rhsData)

    // Then
    result should equal(
      List(
        Map("x" -> nodeValue(42), "y" -> relValue(1337),"z" -> stringValue("FOO")),
        Map("x" -> relValue(43), "y" -> relValue(44),"z" -> relValue(45))
      ))
  }

  private def newMockedNode(id: Long) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  private def newMockedRelationship(id: Long) = {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(id)
    rel
  }

  private def nodeValue(id: Long) = VirtualValues.nodeValue(id, stringArray("L"), EMPTY_MAP)
  private def relValue(id: Long) = VirtualValues.relationshipValue(id, nodeValue(id - 1), nodeValue(id + 1), stringValue("L"), EMPTY_MAP)
}
