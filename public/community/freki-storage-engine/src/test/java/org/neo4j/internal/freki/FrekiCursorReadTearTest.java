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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.stream.LongStream;

import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;
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

    @Disabled
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

    @Disabled
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
