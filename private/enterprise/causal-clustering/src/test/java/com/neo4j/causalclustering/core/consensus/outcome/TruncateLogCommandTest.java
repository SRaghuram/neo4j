/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.LogAssertions.assertThat;

class TruncateLogCommandTest
{
    @Test
    void applyTo()
    {
        //Test that truncate commands correctly remove entries from the cache.

        //given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        long fromIndex = 2L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );
        InFlightCache inFlightCache = new ConsecutiveInFlightCache();

        inFlightCache.put( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightCache.put( 1L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 2L, valueOf( 2 ) ) );
        inFlightCache.put( 3L, new RaftLogEntry( 3L, valueOf( 3 ) ) );

        //when
        truncateLogCommand.applyTo( inFlightCache, log );

        //then
        Assertions.assertNotNull( inFlightCache.get( 0L ) );
        Assertions.assertNotNull( inFlightCache.get( 1L ) );
        Assertions.assertNull( inFlightCache.get( 2L ) );
        Assertions.assertNull( inFlightCache.get( 3L ) );

        assertThat(logProvider).forClass( getClass() ).forLevel( DEBUG )
                .containsMessageWithArguments( "Start truncating in-flight-map from index %d. Current map:%n%s", fromIndex, inFlightCache );
    }

    @Test
    void shouldTruncateWithGaps()
    {
        //given
        long fromIndex = 1L;
        TruncateLogCommand truncateLogCommand = new TruncateLogCommand( fromIndex );

        InFlightCache inFlightCache = new ConsecutiveInFlightCache();

        inFlightCache.put( 0L, new RaftLogEntry( 0L, valueOf( 0 ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 1L, valueOf( 1 ) ) );
        inFlightCache.put( 4L, new RaftLogEntry( 2L, valueOf( 2 ) ) );

        truncateLogCommand.applyTo( inFlightCache, NullLog.getInstance() );

        inFlightCache.put( 1L, new RaftLogEntry( 3L, valueOf( 1 ) ) );
        inFlightCache.put( 2L, new RaftLogEntry( 4L, valueOf( 2 ) ) );

        Assertions.assertNotNull( inFlightCache.get( 1L ) );
        Assertions.assertNotNull( inFlightCache.get( 2L ) );
    }
}
