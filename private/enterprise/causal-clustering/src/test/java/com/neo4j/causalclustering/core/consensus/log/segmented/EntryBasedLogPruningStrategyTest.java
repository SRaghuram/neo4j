/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class EntryBasedLogPruningStrategyTest extends PruningStrategyTest
{
    @Test
    void indexToKeepTest() throws IOException
    {
        // given
        files = createSegmentFiles( 10 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, nullLogProvider() );

        // when
        long indexToKeep = strategy.getIndexToKeep( segments );

        // then
        assertEquals( 2, indexToKeep );
    }

    @Test
    void pruneStrategyExceedsNumberOfEntriesTest() throws IOException
    {
        //given
        files = createSegmentFiles( 10 ).subList( 5, 10 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 7, nullLogProvider() );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 4, indexToKeep );
    }

    @Test
    void onlyFirstActiveLogFileTest() throws IOException
    {
        //given
        files = createSegmentFiles( 1 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, nullLogProvider() );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( -1, indexToKeep );
    }

    @Test
    void onlyOneActiveLogFileTest() throws IOException
    {
        //given
        files = createSegmentFiles( 6 ).subList( 4, 6 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, nullLogProvider() );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 3, indexToKeep );
    }
}
