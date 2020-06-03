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

import java.util.stream.LongStream;

import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.freki.Record.recordDataSize;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

public class FrekiCursorReadTearTest extends FrekiCursorsTest
{
    @Test
    void shouldSeeLabelsMovingRight()
    {
        // given
        FrekiNodeCursor nodeCursorAtV1 = node().labels( intArray( 0, 5 ) ).storeAndPlaceNodeCursorAt();

        // when
        existingNode( nodeCursorAtV1.entityReference() ).labels( intArray( 5, recordDataSize( 0 ) ) ).store();

        // then
        long[] readLabelsAfterWritingV2 = nodeCursorAtV1.labels();
        for ( int i = 0; i < 5; i++ )
        {
            long expectedId = i;
            assertThat( LongStream.of( readLabelsAfterWritingV2 ).filter( id -> id == expectedId ).count() ).isEqualTo( 1 );
        }
    }

    @Test
    void shouldSeeLabelsMovingLeft()
    {
        // given
        FrekiNodeCursor nodeCursorAtV1 = node().labels( intArray( 0, recordDataSize( 0 ) ) ).storeAndPlaceNodeCursorAt();

        // when
        existingNode( nodeCursorAtV1.entityReference() ).removeLabels( intArray( 5, recordDataSize( 0 ) ) ).store();

        // then
        long[] readLabelsAfterWritingV2 = nodeCursorAtV1.labels();
        for ( int i = 0; i < 5; i++ )
        {
            long expectedId = i;
            assertThat( LongStream.of( readLabelsAfterWritingV2 ).filter( id -> id == expectedId ).count() ).isEqualTo( 1 );
        }
    }

    @Test
    void shouldSeePropertiesMovingRight()
    {
        // given
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 1000, stringValue( "abc" ) );
        properties.put( 1001, intValue( 100 ) );
        properties.put( 1002, intValue( 101 ) );
        properties.put( 1003, intValue( 102 ) );
        FrekiNodeCursor nodeCursorAtV1 = node().properties( properties ).storeAndPlaceNodeCursorAt();
        long nodeId = nodeCursorAtV1.entityReference();

        // when pushing out properties to X2, making room for labels in X1
        int nextPropertyKey = 0;
        while ( !matchesPhysicalLayout( nodeId, layout().properties( 1 ) ) )
        {
            Node node = existingNode( nodeId );
            for ( int i = 0; i < 3; i++ )
            {
                node.property( nextPropertyKey++, intValue( random.nextInt() ) );
            }
            node.store();
        }

        // then
        try ( FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            nodeCursorAtV1.properties( propertyCursor );
            MutableIntObjectMap<Value> readProperties = readProperties( propertyCursor );
            assertThat( readProperties ).isEqualTo( properties );
        }
    }

    @Test
    void shouldSeePropertiesMovingLeft()
    {
        // given
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 1000, stringValue( "abc" ) );
        properties.put( 1001, intValue( 100 ) );
        properties.put( 1002, intValue( 101 ) );
        properties.put( 1003, intValue( 102 ) );
        long nodeId = node().properties( properties ).store();
        int nextPropertyKey = 0;
        FrekiNodeCursor nodeCursorAtV1 = null;
        while ( !matchesPhysicalLayout( nodeId, layout().properties( 1 ) ) )
        {
            nodeCursorAtV1 = existingNode( nodeId ).property( nextPropertyKey++, intValue( random.nextInt() ) ).storeAndPlaceNodeCursorAt();
        }

        // when
        int deletePropertyKey = 0;
        while ( !matchesPhysicalLayout( nodeId, layout().properties( 0 ) ) )
        {
            existingNode( nodeId ).removeProperty( deletePropertyKey++ ).storeAndPlaceNodeCursorAt();
        }

        // then
        try ( FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            nodeCursorAtV1.properties( propertyCursor );
            MutableIntObjectMap<Value> readProperties = readProperties( propertyCursor );
            properties.forEachKeyValue( ( key, value ) -> assertThat( readProperties.get( key ) ).isEqualTo( value ) );
        }
    }

    // TODO shouldSeePropertiesMovedLeftAfterReturningSome
    // TODO shouldSeePropertiesMovedRightAfterReturningSome

    @Test
    void shouldSeePropertiesInXLChain()
    {
        // given
        int x8Size = stores.mainStore( 3 ).recordDataSize();
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        Value prop = Values.byteArray( new byte[]{0, 1, 2, 3, 4, 5, 6} ); //this will generate 10B data (header + length + data + key)
        int sizePerProp = 10;
        int propSize = (int) (x8Size * 1.5);
        int nextPropKey = 0;
        for ( int i = 0; i < propSize / sizePerProp; i++ )
        {
            properties.put( nextPropKey++, prop );
        }

        int checkKey = nextPropKey;
        TextValue hello = stringValue( "hello" );
        properties.put( checkKey, hello );

        FrekiNodeCursor nodeCursorAtV1 = node().properties( properties ).storeAndPlaceNodeCursorAt();
        long nodeId = nodeCursorAtV1.entityReference();
        PhysicalLayout layout = capturePhysicalLayout( nodeId );

        int deletePropertyKey = 0;
        try ( FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            nodeCursorAtV1.properties( propertyCursor );
            for ( int i = 0; i < 5; i++ )
            {
                propertyCursor.next(); //Traverse a bit
            }

            while ( matchesPhysicalLayout( nodeId, layout ) )
            {
                existingNode( nodeId ).removeProperty( deletePropertyKey++ ).storeAndPlaceNodeCursorAt();
            }

            //TODO For now we expect this to throw. But eventually we need to support this.
            assertThatThrownBy( () -> readProperties( propertyCursor ) ).hasMessageContaining( "Reading split data from records with different version." );
            //assertThat( readProperties( propertyCursor ) ).contains( hello ); ← This is what we want! But for now ↑
        }
    }

    @Test
    void shouldSeeAllDataWhenMovingMultipleParts()
    {
        // given
        int[] labels = new int[256];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = i;
        }

        Node node = node();
        Node otherNode = node();
        node.labels( labels );
        for ( int i = 0; i < 10; i++ )
        {
            node.relationship( i % 3, node );
        }
        FrekiNodeCursor nodeCursorAtV1 = node.storeAndPlaceNodeCursorAt();
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            nodeCursorAtV1.relationships( relationshipCursor, RelationshipSelection.ALL_RELATIONSHIPS );
            relationshipCursor.next();
            node = existingNode( node.id() );
            node.removeLabels( labels ).store(); //delete labels  XL -> deleted
            for ( int i = 0; i < 1000; i++ )
            {
                node.relationship( i % 3, otherNode );
            }

            nodeCursorAtV1 = node.storeAndPlaceNodeCursorAt();

            assertThat( nodeCursorAtV1.labels() ).isEmpty();
            //Here we should only see the first 10
            for ( int i = 0; i < 9; i++ )
            {
                assertThat( relationshipCursor.next() ).isTrue();
            }
            assertThat( relationshipCursor.next() ).isFalse();

            //And here we see them all
            nodeCursorAtV1.relationships( relationshipCursor, RelationshipSelection.ALL_RELATIONSHIPS );
            for ( int i = 0; i < 1010; i++ )
            {
                assertThat( relationshipCursor.next() ).isTrue();
            }
            assertThat( relationshipCursor.next() ).isFalse();
        }
    }

    @Test
    void shouldFindDataWhenRemovedDataAndRecord()
    {
        //Given
        Node node = node();

        int[] labels = new int[200];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = i;
        }
        node.labels( labels );

        FrekiNodeCursor nodeCursorAtV1 = node.storeAndPlaceNodeCursorAt();

        //when
        node = existingNode( node.id() );
        node.removeLabels( labels );
        FrekiNodeCursor nodeCursorAtV2 = node.storeAndPlaceNodeCursorAt();

        //then
        assertThat( nodeCursorAtV1.labels() ).isEmpty();
        assertThat( nodeCursorAtV2.labels() ).isEmpty();
    }

    @Test
    void shouldFindDataWhenMovedToNewRecord()
    {
        //Given
        Node node = node();

        int[] labels = new int[200];
        int[] moreLabels = new int[200];

        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = i;
            moreLabels[i] = labels.length + i;
            node.relationship( 0, node );
        }
        node.labels( labels );

        FrekiNodeCursor nodeCursorAtV1 = node.storeAndPlaceNodeCursorAt();

        //when
        node = existingNode( node.id() );
        node.labels( moreLabels );
        FrekiNodeCursor nodeCursorAtV2 = node.storeAndPlaceNodeCursorAt();

        //then

        assertThat( nodeCursorAtV1.labels() ).hasSize( 400 );
        assertThat( nodeCursorAtV2.labels() ).hasSize( 400 );
    }

    private int[] intArray( int from, int to )
    {
        int[] labelsLargerThanX1 = new int[to - from];
        for ( int i = 0; i < labelsLargerThanX1.length; i++ )
        {
            labelsLargerThanX1[i] = from + i;
        }
        return labelsLargerThanX1;
    }
}
