/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

class RaftLogMetadataCacheTest
{
    @Test
    void shouldReturnNullWhenMissingAnEntryInTheCache()
    {
        // given
        final RaftLogMetadataCache cache = new RaftLogMetadataCache( 2 );

        // when
        final RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( 42 );

        // then
        Assertions.assertNull( metadata );
    }

    @Test
    void shouldReturnTheTxValueTIfInTheCached()
    {
        // given
        final RaftLogMetadataCache cache = new RaftLogMetadataCache( 2 );
        final long index = 12L;
        final long term = 12L;
        final LogPosition position = new LogPosition( 3, 4 );

        // when
        cache.cacheMetadata( index, term, position );
        final RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( index );

        // then
        Assertions.assertEquals( new RaftLogMetadataCache.RaftLogEntryMetadata( term, position ), metadata );
    }

    @Test
    void shouldClearTheCache()
    {
        // given
        final RaftLogMetadataCache cache = new RaftLogMetadataCache( 2 );
        final long index = 12L;
        final long term = 12L;
        final LogPosition position = new LogPosition( 3, 4 );

        // when
        cache.cacheMetadata( index, term, position );
        cache.clear();
        RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( index );

        // then
        Assertions.assertNull( metadata );
    }

    @Test
    void shouldRemoveUpTo()
    {
        // given
        int cacheSize = 100;
        RaftLogMetadataCache cache = new RaftLogMetadataCache( cacheSize );

        for ( int i = 0; i < cacheSize; i++ )
        {
            cache.cacheMetadata( i, i, new LogPosition( i, i ) );
        }

        // when
        int upTo = 30;
        cache.removeUpTo( upTo );

        // then
        long i = 0;
        for ( ; i <= upTo; i++ )
        {
            Assertions.assertNull( cache.getMetadata( i ) );
        }
        for ( ; i < cacheSize; i++ )
        {
            RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( i );
            Assertions.assertNotNull( metadata );
            Assertions.assertEquals( i, metadata.getEntryTerm() );
        }
    }

    @Test
    void shouldRemoveUpwardsFrom()
    {
        // given
        int cacheSize = 100;
        RaftLogMetadataCache cache = new RaftLogMetadataCache( cacheSize );

        for ( int i = 0; i < cacheSize; i++ )
        {
            cache.cacheMetadata( i, i, new LogPosition( i, i ) );
        }

        // when
        int upFrom = 60;
        cache.removeUpwardsFrom( upFrom );

        // then
        long i = 0;
        for ( ; i < upFrom; i++ )
        {
            RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( i );
            Assertions.assertNotNull( metadata );
            Assertions.assertEquals( i, metadata.getEntryTerm() );
        }
        for ( ; i < cacheSize; i++ )
        {
            Assertions.assertNull( cache.getMetadata( i ) );
        }
    }

    @Test
    void shouldAcceptAndReturnIndexesInRangeJustDeleted()
    {
        // given
        int cacheSize = 100;
        RaftLogMetadataCache cache = new RaftLogMetadataCache( cacheSize );

        for ( int i = 0; i < cacheSize; i++ )
        {
            cache.cacheMetadata( i, i, new LogPosition( i, i ) );
        }

        // when
        int upFrom = 60;
        cache.removeUpwardsFrom( upFrom );

        // and we add something in the deleted range
        int insertedIndex = 70;
        long insertedTerm = 150;
        cache.cacheMetadata( insertedIndex, insertedTerm, new LogPosition( insertedIndex, insertedIndex ) );

        // then
        // nothing should be resurrected in the deleted range just because we inserted something there
        int i = upFrom;
        for ( ; i < insertedIndex; i++ )
        {
            Assertions.assertNull( cache.getMetadata( i ) );
        }
        // i here should be insertedIndex
        Assertions.assertEquals( insertedTerm, cache.getMetadata( i ).getEntryTerm() );
        i++; // to continue iteration in the rest of the deleted range
        for (; i < cacheSize; i++ )
        {
            Assertions.assertNull( cache.getMetadata( i ) );
        }
    }
}
