/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * This cache is not meant for production use, but it can be useful
 * in various investigative circumstances.
 */
public class UnboundedInFlightCache implements InFlightCache
{
    private Map<Long,RaftLogEntry> map = new HashMap<>();
    private boolean enabled;

    @Override
    public synchronized void enable()
    {
        enabled = true;
    }

    @Override
    public synchronized void put( long logIndex, RaftLogEntry entry )
    {
        if ( !enabled )
        {
            return;
        }

        map.put( logIndex, entry );
    }

    @Override
    public synchronized RaftLogEntry get( long logIndex )
    {
        if ( !enabled )
        {
            return null;
        }

        return map.get( logIndex );
    }

    @Override
    public synchronized void truncate( long fromIndex )
    {
        if ( !enabled )
        {
            return;
        }

        map.keySet().removeIf( idx -> idx >= fromIndex );
    }

    @Override
    public synchronized void prune( long upToIndex )
    {
        if ( !enabled )
        {
            return;
        }

        map.keySet().removeIf( idx -> idx <= upToIndex );
    }

    @Override
    public synchronized long totalBytes()
    {
        // not updated correctly
        return 0;
    }

    @Override
    public synchronized int elementCount()
    {
        // not updated correctly
        return 0;
    }

    @Override
    public void reportSkippedCacheAccess()
    {
    }
}
