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

import org.junit.jupiter.api.Test;

import org.neo4j.storageengine.api.StorageCursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.values.storable.Values.intValue;

class FrekiCursorInitializationTest extends FrekiCursorsTest
{
    @Test
    void nodeCursorShouldReturnFalseOnNextAfterReset()
    {
        assertNextOnUninitializedCursor( cursorFactory.allocateNodeCursor( NULL ) );
    }

    @Test
    void nodeCursorShouldContinueToReturnFalseAfterFinalNext()
    {
        // given
        var node = node().store();

        // when
        var cursor = cursorFactory.allocateNodeCursor( NULL );
        cursor.single( node.id );
        assertThat( cursor.next() ).isTrue();
        assertThat( cursor.next() ).isFalse();

        // then
        assertThat( cursor.next() ).isFalse();
    }

    @Test
    void propertyCursorShouldReturnFalseOnNextAfterReset()
    {
        assertNextOnUninitializedCursor( cursorFactory.allocatePropertyCursor( NULL ) );
    }

    @Test
    void propertyCursorShouldContinueToReturnFalseAfterFinalNext()
    {
        // given
        var nodeCursor = node().property( 0, intValue( 10 ) ).storeAndPlaceNodeCursorAt();
        var cursor = cursorFactory.allocatePropertyCursor( NULL );
        nodeCursor.properties( cursor );

        // when
        assertThat( cursor.next() ).isTrue();
        assertThat( cursor.propertyValue() ).isEqualTo( intValue( 10 ) );
        assertThat( cursor.next() ).isFalse();

        // then
        assertThat( cursor.next() ).isFalse();
    }

    @Test
    void relationshipScanCursorShouldReturnFalseOnNextAfterReset()
    {
        assertNextOnUninitializedCursor( cursorFactory.allocateRelationshipScanCursor( NULL ) );
    }

    @Test
    void relationshipScanCursorShouldContinueToReturnFalseAfterFinalNext()
    {
        // given
        Node node1 = node();
        Node node2 = node();
        long relationshipId = node1.relationshipAndReturnItsId( 0, node2 );
        node1.store();
        node2.store();
        var cursor = cursorFactory.allocateRelationshipScanCursor( NULL );
        cursor.single( relationshipId );

        // when
        assertThat( cursor.next() ).isTrue();
        assertThat( cursor.next() ).isFalse();

        // then
        assertThat( cursor.next() ).isFalse();
    }

    @Test
    void relationshipTraversalCursorShouldReturnFalseOnNextAfterReset()
    {
        assertNextOnUninitializedCursor( cursorFactory.allocateRelationshipTraversalCursor( NULL ) );
    }

    @Test
    void relationshipTraversalCursorShouldContinueToReturnFalseAfterFinalNext()
    {
        // given
        Node node1 = node();
        Node node2 = node();
        long relationshipId = node1.relationshipAndReturnItsId( 0, node2 );
        FrekiNodeCursor nodeCursor = node1.storeAndPlaceNodeCursorAt();
        node2.store();
        var cursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
        nodeCursor.relationships( cursor, ALL_RELATIONSHIPS );

        // when
        assertThat( cursor.next() ).isTrue();
        assertThat( cursor.next() ).isFalse();

        // then
        assertThat( cursor.next() ).isFalse();
    }

    private void assertNextOnUninitializedCursor( StorageCursor cursor )
    {
        assertThat( cursor.next() ).isFalse();
        cursor.reset();
        assertThat( cursor.next() ).isFalse();
    }
}
