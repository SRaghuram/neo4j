/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers._
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext}
import org.neo4j.cypher.internal.v4_0.util.CypherTypeException
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, RelationshipScanCursor}
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values._
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

class CompiledHelpersTest extends CypherFunSuite {

  test("assertBooleanOrNoValue") {
    assertBooleanOrNoValue(TRUE) should equal(TRUE)
    assertBooleanOrNoValue(FALSE) should equal(FALSE)
    assertBooleanOrNoValue(NO_VALUE) should equal(NO_VALUE)
    a[CypherTypeException] should be thrownBy assertBooleanOrNoValue(PI)
  }

  test("cachedNodeProperty should return NO_VALUE for missing node and missing property") {
    //given
    val nodeOffset = 42
    val noNodeOffset = 43
    val context = mock[ExecutionContext]

    //when
    when(context.getLongAt(noNodeOffset)).thenReturn(-1)
    when(context.getLongAt(nodeOffset)).thenReturn(1)

    //then
    cachedNodePropertyWithLongSlot(context, mock[DbAccess],
                   noNodeOffset,
                   1337, 11,
                   mock[NodeCursor],
                   mock[PropertyCursor]) should equal(NO_VALUE)
    cachedNodePropertyWithLongSlot(context, mock[DbAccess],
                   nodeOffset,
                   -1, 11,
                   mock[NodeCursor],
                   mock[PropertyCursor]) should equal(NO_VALUE)
  }

  test("cachedRelationshipProperty should return NO_VALUE for missing node and missing property") {
    //given
    val relationshipOffset = 42
    val noRelationshipOffset = 43
    val context = mock[ExecutionContext]

    //when
    when(context.getLongAt(noRelationshipOffset)).thenReturn(-1)
    when(context.getLongAt(relationshipOffset)).thenReturn(1)

    //then
    cachedRelationshipPropertyWithLongSlot(context, mock[DbAccess],
      noRelationshipOffset,
                   1337, 11,
                   mock[RelationshipScanCursor],
                   mock[PropertyCursor]) should equal(NO_VALUE)
    cachedRelationshipPropertyWithLongSlot(context, mock[DbAccess],
      relationshipOffset,
                   -1, 11,
                   mock[RelationshipScanCursor],
                   mock[PropertyCursor]) should equal(NO_VALUE)
  }

  test("cachedNodeProperty should return from tx state") {
    //given
    val nodeOffset = 42
    val context = mock[ExecutionContext]
    val nodeId = 1
    val access = mock[DbAccess]
    val property = 11

    //when
    when(context.getLongAt(nodeOffset)).thenReturn(nodeId)
    when(access.getTxStateNodePropertyOrNull(nodeId, property)).thenReturn(PI)

    //then
    cachedNodePropertyWithLongSlot(context, access,
                   nodeOffset,
                   property, 11,
                   mock[NodeCursor],
                   mock[PropertyCursor]) should equal(PI)

  }

  test("cachedRelationshipProperty should return from tx state") {
    //given
    val relationshipOffset = 42
    val context = mock[ExecutionContext]
    val relationshipId = 1
    val access = mock[DbAccess]
    val property = 11

    //when
    when(context.getLongAt(relationshipOffset)).thenReturn(relationshipId)
    when(access.getTxStateRelationshipPropertyOrNull(relationshipId, property)).thenReturn(PI)

    //then
    cachedRelationshipPropertyWithLongSlot(context, access,
      relationshipOffset,
                   property, 11,
                   mock[RelationshipScanCursor],
                   mock[PropertyCursor]) should equal(PI)

  }

  test("cachedNodeProperty should return cached if not in tx state") {
    //given
    val nodeOffset = 42
    val context = mock[ExecutionContext]
    val nodeId = 1
    val access = mock[DbAccess]
    val property = 11
    val propertyOffset = 11

    //when
    when(context.getLongAt(nodeOffset)).thenReturn(nodeId)
    when(access.getTxStateNodePropertyOrNull(nodeId, property)).thenReturn(null)
    when(context.getCachedPropertyAt(propertyOffset)).thenReturn(PI)

    //then
    cachedNodePropertyWithLongSlot(context, access,
                   nodeOffset,
                   property, propertyOffset,
                   mock[NodeCursor],
                   mock[PropertyCursor]) should equal(PI)

  }

  test("cachedRelationshipProperty should return cached if not in tx state") {
    //given
    val relationshipOffset = 42
    val context = mock[ExecutionContext]
    val relationshipId = 1
    val access = mock[DbAccess]
    val property = 11
    val propertyOffset = 11

    //when
    when(context.getLongAt(relationshipOffset)).thenReturn(relationshipId)
    when(access.getTxStateRelationshipPropertyOrNull(relationshipId, property)).thenReturn(null)
    when(context.getCachedPropertyAt(propertyOffset)).thenReturn(PI)

    //then
    cachedRelationshipPropertyWithLongSlot(context, access,
      relationshipOffset,
                   property, propertyOffset,
                   mock[RelationshipScanCursor],
                   mock[PropertyCursor]) should equal(PI)

  }

  test("cachedNodeProperty should get from store if not in tx state nor in cache, and re-cache") {
    //given
    val nodeOffset = 42
    val context = mock[ExecutionContext]
    val nodeId = 1
    val access = mock[DbAccess]
    val property = 11
    val propertyOffset = 11
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]

    //when
    when(context.getLongAt(nodeOffset)).thenReturn(nodeId)
    when(access.getTxStateNodePropertyOrNull(nodeId, property)).thenReturn(null)
    when(context.getCachedPropertyAt(propertyOffset)).thenReturn(null)
    when(access.nodeProperty(nodeId, property, nodeCursor, propertyCursor, true)).thenReturn(PI)

    //then
    cachedNodePropertyWithLongSlot(context, access,
                   nodeOffset,
                   property, propertyOffset,
                   nodeCursor,
                   propertyCursor) should equal(PI)

    verify(context, times(1)).setCachedPropertyAt(propertyOffset, PI)
  }

  test("cachedRelationshipProperty should get from store if not in tx state nor in cache, and re-cache") {
    //given
    val relationshipOffset = 42
    val context = mock[ExecutionContext]
    val relationshipId = 1
    val access = mock[DbAccess]
    val property = 11
    val propertyOffset = 11
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]

    //when
    when(context.getLongAt(relationshipOffset)).thenReturn(relationshipId)
    when(access.getTxStateRelationshipPropertyOrNull(relationshipId, property)).thenReturn(null)
    when(context.getCachedPropertyAt(propertyOffset)).thenReturn(null)
    when(access.relationshipProperty(relationshipId, property, relationshipCursor, propertyCursor, true)).thenReturn(PI)

    //then
    cachedRelationshipPropertyWithLongSlot(context, access,
      relationshipOffset,
                   property, propertyOffset,
      relationshipCursor,
                   propertyCursor) should equal(PI)

    verify(context, times(1)).setCachedPropertyAt(propertyOffset, PI)
  }

  test("cachedNodePropertyExists should get from store if not in tx state nor in cache, and re-cache") {
    //given
    val nodeOffset = 42
    val context = mock[ExecutionContext]
    val nodeId = 1
    val access = mock[DbAccess]
    val property = 11
    val propertyOffset = 11
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]

    //when
    when(context.getLongAt(nodeOffset)).thenReturn(nodeId)
    when(access.getTxStateNodePropertyOrNull(nodeId, property)).thenReturn(null)
    when(context.getCachedPropertyAt(propertyOffset)).thenReturn(null)
    when(access.nodeProperty(nodeId, property, nodeCursor, propertyCursor, false)).thenReturn(PI)

    //then
    cachedNodePropertyExistsWithLongSlot(context, access,
                   nodeOffset,
                   property, propertyOffset,
                   nodeCursor,
                   propertyCursor) should equal(Values.TRUE)

    verify(context, times(1)).setCachedPropertyAt(propertyOffset, PI)
  }

  test("cachedRelationshipPropertyExists should get from store if not in tx state nor in cache, and re-cache") {
    //given
    val relationshipOffset = 42
    val context = mock[ExecutionContext]
    val relationshipId = 1
    val access = mock[DbAccess]
    val property = 11
    val propertyOffset = 11
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]

    //when
    when(context.getLongAt(relationshipOffset)).thenReturn(relationshipId)
    when(access.getTxStateRelationshipPropertyOrNull(relationshipId, property)).thenReturn(null)
    when(context.getCachedPropertyAt(propertyOffset)).thenReturn(null)
    when(access.relationshipProperty(relationshipId, property, relationshipCursor, propertyCursor, false)).thenReturn(PI)

    //then
    cachedRelationshipPropertyExistsWithLongSlot(context, access,
      relationshipOffset,
                   property, propertyOffset,
      relationshipCursor,
                   propertyCursor) should equal(Values.TRUE)

    verify(context, times(1)).setCachedPropertyAt(propertyOffset, PI)
  }

  test("nodeOrNoValue") {
    //given
    val nodeOffset = 42
    val noNodeOffset = 43
    val context = mock[ExecutionContext]
    val nodeId = 1
    val access = mock[DbAccess]
    val node = mock[NodeValue]

    //when
    when(context.getLongAt(nodeOffset)).thenReturn(nodeId)
    when(context.getLongAt(noNodeOffset)).thenReturn(-1L)
    when(access.nodeById(nodeId)).thenReturn(node)

    //then
    nodeOrNoValue(context, access, nodeOffset) should equal(node)
    nodeOrNoValue(context, access, noNodeOffset) should equal(NO_VALUE)
  }

  test("relationshipOrNoValue") {
    //given
    val relOffset = 42
    val noRelOffset = 43
    val context = mock[ExecutionContext]
    val relId = 1
    val access = mock[DbAccess]
    val relationship = mock[RelationshipValue]

    //when
    when(context.getLongAt(relOffset)).thenReturn(relId)
    when(context.getLongAt(noRelOffset)).thenReturn(-1L)
    when(access.relationshipById(relId)).thenReturn(relationship)

    //then
    relationshipOrNoValue(context, access, relOffset) should equal(relationship)
    relationshipOrNoValue(context, access, noRelOffset) should equal(NO_VALUE)
  }
}
