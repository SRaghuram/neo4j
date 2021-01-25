/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.cache;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

/**
 * A cache for in-flight entries which also tracks the size of the cache.
 */
public interface InFlightCache
{
    /**
     * Enables the cache.
     */
    void enable();

    /**
     * Put item into the cache.
     *
     * @param logIndex the index of the log entry.
     * @param entry the Raft log entry.
     */
    void put( long logIndex, RaftLogEntry entry );

    /**
     * Get item from the cache.
     *
     * @param logIndex the index of the log entry.
     * @return the log entry.
     */
    RaftLogEntry get( long logIndex );

    /**
     * Disposes of a range of elements from the tail of the consecutive cache.
     *
     * @param fromIndex the index to start from (inclusive).
     */
    void truncate( long fromIndex );

    /**
     * Prunes items from the cache.
     *
     * @param upToIndex the last index to prune (inclusive).
     */
    void prune( long upToIndex );

    /**
     * @return the amount of data in the cache.
     */
    long totalBytes();

    /**
     * @return the number of log entries in the cache.
     */
    int elementCount();

    /**
     * Signal that an external component decided to not access the cache but fallback to a slower path to retrieve a {@link RaftLogEntry}.
     */
    void reportSkippedCacheAccess();
}
