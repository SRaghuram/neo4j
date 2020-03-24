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

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;
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
        var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
        nodeCursor.properties( propertyCursor );

        // when
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 1 );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value1 );
        assertThat( nodeCursor.labels() ).isEqualTo( new long[]{1, 2, 5, 6} );

        // then
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 2 );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value2 );
    }

    @Test
    void shouldHandleIteratingPropertyValueInterleavedWithLabels()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        var nodeCursor = node().labels( 1, 2, 5, 6 ).property( 1, value1 ).property( 2, value2 ).storeAndPlaceNodeCursorAt();
        var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
        nodeCursor.properties( propertyCursor );

        // when
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 1 );
        assertThat( nodeCursor.labels() ).isEqualTo( new long[]{1, 2, 5, 6} );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value1 );

        // then
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 2 );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value2 );
    }

    @Test
    void shouldHandleIteratingPropertiesInterleavedWithRelationships()
    {
        // given
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 20 );
        var node1 = node();
        var node2 = node();
        node1.store();
        node2.store();
        var node = node().property( 1, value1 ).property( 2, value2 );
        node.relationship( 1, node1 );
        node.relationship( 2, node2 );
        var nodeCursor = node.storeAndPlaceNodeCursorAt();
        var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
        var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL );
        nodeCursor.properties( propertyCursor );

        // when
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 1 );
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value1 );
        assertThat( relationshipCursor.next() ).isTrue();
        assertThat( relationshipCursor.type() ).isEqualTo( 1 );
        assertThat( relationshipCursor.neighbourNodeReference() ).isEqualTo( node1.id() );

        // then
        assertThat( propertyCursor.next() ).isTrue();
        assertThat( propertyCursor.propertyKey() ).isEqualTo( 2 );
        assertThat( propertyCursor.propertyValue() ).isEqualTo( value2 );
        assertThat( relationshipCursor.next() ).isTrue();
        assertThat( relationshipCursor.type() ).isEqualTo( 2 );
        assertThat( relationshipCursor.neighbourNodeReference() ).isEqualTo( node2.id() );
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
        node1.store();
        node2.store();
        var node = node().property( 1, value1 ).property( 2, value2 );
        node.relationship( 1, node1, properties );
        node.relationship( 2, node2, properties );
        var nodeCursor = node.storeAndPlaceNodeCursorAt();
        var propertyCursor = cursorFactory.allocatePropertyCursor( PageCursorTracer.NULL );
        var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( PageCursorTracer.NULL );
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
}
