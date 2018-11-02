/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InFlightLogEntriesCacheTest
{
    @Test
    public void shouldNotCacheUntilEnabled()
    {
        InFlightMap<Object> cache = new InFlightMap<>();
        Object entry = new Object();

        cache.put( 1L, entry );
        assertNull( cache.get( 1L ) );

        cache.enable();
        cache.put( 1L, entry );
        assertEquals( entry, cache.get( 1L ) );
    }

    @Test
    public void shouldRegisterAndUnregisterValues()
    {
        InFlightMap<Object> entries = new InFlightMap<>();
        entries.enable();

        Map<Long, Object> logEntryList = new HashMap<>();
        logEntryList.put(1L, new Object() );

        for ( Map.Entry<Long, Object> entry : logEntryList.entrySet() )
        {
            entries.put( entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<Long, Object> entry : logEntryList.entrySet() )
        {
            Object retrieved = entries.get( entry.getKey() );
            assertEquals( entry.getValue(), retrieved );
        }

        Long unexpected = 2L;
        Object shouldBeNull = entries.get( unexpected );
        assertNull( shouldBeNull );

        for ( Map.Entry<Long, Object> entry : logEntryList.entrySet() )
        {
            boolean wasThere = entries.remove( entry.getKey() );
            assertTrue( wasThere );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotReinsertValues()
    {
        InFlightMap<Object> entries = new InFlightMap<>();
        entries.enable();
        Object addedObject = new Object();
        entries.put( 1L, addedObject );
        entries.put( 1L, addedObject );
    }

    @Test
    public void shouldNotReplaceRegisteredValues()
    {
        InFlightMap<Object> cache = new InFlightMap<>();
        cache.enable();
        Object first = new Object();
        Object second = new Object();

        try
        {
            cache.put( 1L, first );
            cache.put( 1L, second );
            fail("Should not allow silent replacement of values");
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( first, cache.get( 1L ) );
        }
    }
}
