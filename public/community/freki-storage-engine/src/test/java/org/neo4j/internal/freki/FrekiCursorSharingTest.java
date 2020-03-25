/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.Test;

import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.values.storable.Values.intValue;

class FrekiCursorSharingTest extends FrekiCursorsTest
{
    @Test
    void shouldHandleIteratingPropertiesInterleavedWithLabels()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        var nodeCursor = node().labels( 1, 2, 5, 6 ).property( 1, value1 ).property( 2, value2 ).storeAndPlaceNodeCursorAt();
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        nodeCursor.properties( propertyCursor );

        // when
        assertNextProperty( propertyCursor, 1, value1 );
        assertThat( nodeCursor.labels() ).isEqualTo( new long[]{1, 2, 5, 6} );

        // then
        assertNextProperty( propertyCursor, 2, value2 );
    }

    @Test
    void shouldHandleIteratingPropertyValueInterleavedWithLabels()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        var nodeCursor = node().labels( 1, 2, 5, 6 ).property( 1, value1 ).property( 2, value2 ).storeAndPlaceNodeCursorAt();
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        nodeCursor.properties( propertyCursor );

        // when
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 1 );
        assertThat( nodeCursor.labels() ).isEqualTo( new long[]{1, 2, 5, 6} );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value1 );

        // then
        assertNextProperty( propertyCursor, 2, value2 );
    }

    @Test
    void shouldHandleIteratingPropertiesInterleavedWithRelationships()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        var node1 = node();
        var node2 = node();
        var nodeCursor = node().property( 1, value1 ).property( 2, value2 ).relationship( 1, node1 ).relationship( 2, node2 ).storeAndPlaceNodeCursorAt();
        node1.store();
        node2.store();
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.properties( propertyCursor );

        // when
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 1 );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value1 );
        assertNextRelationship( relationshipCursor, 1, node1.id() );

        // then
        assertNextProperty( propertyCursor, 2, value2 );
        assertNextRelationship( relationshipCursor, 2, node2.id() );
    }

    @Test
    void shouldHandleIteratingRelationshipsInterleavedWithTheirProperties()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 1, value1 );
        properties.put( 2, value2 );
        var node1 = node();
        var node2 = node();
        var nodeCursor = node().property( 1, value1 ).property( 2, value2 ).relationship( 1, node1, properties ).relationship( 2, node2,
                properties ).storeAndPlaceNodeCursorAt();
        node1.store();
        node2.store();
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );

        // when
        assertThat( relationshipCursor.next() ).isTrue();
        assertThat( relationshipCursor.type() ).isEqualTo( 1 );
        relationshipCursor.properties( propertyCursor );
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value1 );
        assertThat( relationshipCursor.next() ).isTrue();
        assertThat( relationshipCursor.type() ).isEqualTo( 2 );

        // then
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value2 );
    }

    @Test
    void shouldLoadIntoNewRecordWhenPropertyCursorNotExhausted()
    {
        // given
        var value1v1 = intValue( 1 );
        var value2v1 = intValue( 2 );
        var value1v2 = intValue( 10 );
        var value2v2 = intValue( 20 );
        var nodeCursor = node().property( 1, value1v1 ).property( 2, value2v1 ).storeAndPlaceNodeCursorAt();
        var otherNode = node().property( 1, value1v2 ).property( 2, value2v2 ).store();
        var firstPropertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        nodeCursor.properties( firstPropertyCursor );
        assertNextProperty( firstPropertyCursor, 1, value1v1 );

        // when
        nodeCursor.single( otherNode.id );
        assertThat( nodeCursor.next() ).isTrue();
        var secondPropertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        nodeCursor.properties( secondPropertyCursor );
        assertNextProperty( secondPropertyCursor, 1, value1v2 );
        assertNextProperty( secondPropertyCursor, 2, value2v2 );

        // then
        assertNextProperty( firstPropertyCursor, 2, value2v1 );
        assertThat( accessPatternTracer.access.getNumberOfNodeLoads() ).isEqualTo( accessPatternTracer.access.getNumberOfReusableNodeLoads() + 1 );
    }

    @Test
    void shouldLoadIntoNewRecordWhenRelationshipCursorNotExhausted()
    {
        // given
        var node2 = node();
        var node3 = node();
        var nodeCursor = node().relationship( 1, node2 ).relationship( 2, node3 ).storeAndPlaceNodeCursorAt();
        long nodeId = nodeCursor.entityReference();
        node2.store();
        node3.store();
        var firstRelationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( firstRelationshipCursor, ALL_RELATIONSHIPS );
        assertNextRelationship( firstRelationshipCursor, 1, node2.id() );

        // when
        nodeCursor.single( node2.id() );
        assertThat( nodeCursor.next() ).isTrue();
        var secondRelationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( secondRelationshipCursor, ALL_RELATIONSHIPS );

        // then
        assertNextRelationship( firstRelationshipCursor, 2, node3.id() );
        assertNextRelationship( secondRelationshipCursor, 1, nodeId );
        assertThat( accessPatternTracer.access.getNumberOfNodeLoads() ).isEqualTo( accessPatternTracer.access.getNumberOfReusableNodeLoads() + 1 );
    }

    @Test
    void shouldLoadIntoNewRecordWhenRelationshipPropertyCursorNotExhausted()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 1, value1 );
        properties.put( 2, value2 );
        var node2 = node();
        var node3 = node();
        var nodeCursor = node().relationship( 1, node2, properties ).relationship( 2, node3, properties ).storeAndPlaceNodeCursorAt();
        long nodeId = nodeCursor.entityReference();
        node2.store();
        node3.store();
        var firstRelationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( firstRelationshipCursor, ALL_RELATIONSHIPS );
        assertNextRelationship( firstRelationshipCursor, 1, node2.id() );
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        firstRelationshipCursor.properties( propertyCursor );
        assertNextProperty( propertyCursor, 1, value1 );
        assertNextRelationship( firstRelationshipCursor, 2, node3.id() );
        assertThat( firstRelationshipCursor.next() ).isFalse();
        // here the node->relationship cursor is exhausted, but the node->relationship->property cursor still remains holding this node data

        // when
        nodeCursor.single( node2.id() );
        assertThat( nodeCursor.next() ).isTrue();
        var secondRelationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( secondRelationshipCursor, ALL_RELATIONSHIPS );

        // then
        assertNextRelationship( secondRelationshipCursor, 1, nodeId );
        assertNextProperty( propertyCursor, 2, value2 );
        assertThat( accessPatternTracer.access.getNumberOfNodeLoads() ).isEqualTo( accessPatternTracer.access.getNumberOfReusableNodeLoads() + 1 );
    }

    @Test
    void shouldReuseRecordAfterSharedPropertyCursorExhausted()
    {
        // given
        var value1v1 = intValue( 1 );
        var value2v1 = intValue( 2 );
        var value1v2 = intValue( 10 );
        var value2v2 = intValue( 20 );
        var nodeCursor = node().property( 1, value1v1 ).property( 2, value2v1 ).storeAndPlaceNodeCursorAt();
        var otherNode = node().property( 1, value1v2 ).property( 2, value2v2 ).store();
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        nodeCursor.properties( propertyCursor );
        assertNextProperty( propertyCursor, 1, value1v1 );
        assertNextProperty( propertyCursor, 2, value2v1 );
        assertThat( propertyCursor.next() ).isFalse();

        // when
        nodeCursor.single( otherNode.id );
        assertThat( nodeCursor.next() ).isTrue();
        nodeCursor.properties( propertyCursor );
        assertNextProperty( propertyCursor, 1, value1v2 );
        assertNextProperty( propertyCursor, 2, value2v2 );

        // then
        assertThat( accessPatternTracer.access.getNumberOfNodeLoads() ).isEqualTo( accessPatternTracer.access.getNumberOfReusableNodeLoads() );
    }

    @Test
    void shouldReuseRecordAfterSharedRelationshipCursorExhausted()
    {
        // given
        var node2 = node();
        var node3 = node();
        var nodeCursor = node().relationship( 1, node2 ).relationship( 2, node3 ).storeAndPlaceNodeCursorAt();
        long nodeId = nodeCursor.entityReference();
        node2.store();
        node3.store();
        var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        assertNextRelationship( relationshipCursor, 1, node2.id() );
        assertNextRelationship( relationshipCursor, 2, node3.id() );
        assertThat( relationshipCursor.next() ).isFalse();

        // when
        nodeCursor.single( node2.id() );
        assertThat( nodeCursor.next() ).isTrue();
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );

        // then
        assertNextRelationship( relationshipCursor, 1, nodeId );
        assertThat( accessPatternTracer.access.getNumberOfNodeLoads() ).isEqualTo( accessPatternTracer.access.getNumberOfReusableNodeLoads() );
    }

    @Test
    void shouldReuseRecordAfterSharedRelationshipCursorWithPropertiesExhausted()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 1, value1 );
        properties.put( 2, value2 );
        var node2 = node();
        var node3 = node();
        var nodeCursor = node().relationship( 1, node2, properties ).relationship( 2, node3, properties ).storeAndPlaceNodeCursorAt();
        long nodeId = nodeCursor.entityReference();
        node2.store();
        node3.store();
        var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        assertNextRelationship( relationshipCursor, 1, node2.id() );
        var propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        relationshipCursor.properties( propertyCursor );
        assertNextProperty( propertyCursor, 1, value1 );
        assertNextProperty( propertyCursor, 2, value2 );
        assertThat( propertyCursor.next() ).isFalse();
        assertNextRelationship( relationshipCursor, 2, node3.id() );
        assertThat( relationshipCursor.next() ).isFalse();

        // when
        nodeCursor.single( node2.id() );
        assertThat( nodeCursor.next() ).isTrue();
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );

        // then
        assertNextRelationship( relationshipCursor, 1, nodeId );
        relationshipCursor.properties( propertyCursor );
        assertNextProperty( propertyCursor, 1, value1 );
        assertNextProperty( propertyCursor, 2, value2 );
        assertThat( propertyCursor.next() ).isFalse();
        assertThat( accessPatternTracer.access.getNumberOfNodeLoads() ).isEqualTo( accessPatternTracer.access.getNumberOfReusableNodeLoads() );
    }

    private void assertNextProperty( FrekiPropertyCursor propertyCursor, int key, Value value )
    {
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( key );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value );
    }

    private void assertNextRelationship( FrekiRelationshipTraversalCursor relationshipCursor, int type, long otherNodeId )
    {
        assertThat( relationshipCursor.next() ).isTrue();
        assertThat( relationshipCursor.type() ).isEqualTo( type );
        assertThat( relationshipCursor.neighbourNodeReference() ).isEqualTo( otherNodeId );
    }
}
