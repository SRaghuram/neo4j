/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

/**
 * A cache which caches nothing. This means that all lookups
 * will go to the on-disk Raft log, which might be quite good
 * anyway since recently written items will be in OS page cache
 * memory generally. But it will incur an unmarshalling overhead.
 */
public class VoidInFlightCache implements InFlightCache
{
    @Override
    public void enable()
    {
    }

    @Override
    public void put( long logIndex, RaftLogEntry entry )
    {
    }

    @Override
    public RaftLogEntry get( long logIndex )
    {
        return null;
    }

    @Override
    public void truncate( long fromIndex )
    {
    }

    @Override
    public void prune( long upToIndex )
    {
    }

    @Override
    public long totalBytes()
    {
        return 0;
    }

    @Override
    public int elementCount()
    {
        return 0;
    }

    @Override
    public void reportSkippedCacheAccess()
    {
    }
}
