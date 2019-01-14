/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class EntryBasedLogPruningStrategyTest extends PruningStrategyTest
{
    @Test
    public void indexToKeepTest()
    {
        // given
        files = createSegmentFiles( 10 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        // when
        long indexToKeep = strategy.getIndexToKeep( segments );

        // then
        assertEquals( 2, indexToKeep );
    }

    @Test
    public void pruneStrategyExceedsNumberOfEntriesTest()
    {
        //given
        files = createSegmentFiles( 10 ).subList( 5, 10 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 7, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 4, indexToKeep );
    }

    @Test
    public void onlyFirstActiveLogFileTest()
    {
        //given
        files = createSegmentFiles( 1 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( -1, indexToKeep );
    }

    @Test
    public void onlyOneActiveLogFileTest()
    {
        //given
        files = createSegmentFiles( 6 ).subList( 4, 6 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 3, indexToKeep );
    }
}
