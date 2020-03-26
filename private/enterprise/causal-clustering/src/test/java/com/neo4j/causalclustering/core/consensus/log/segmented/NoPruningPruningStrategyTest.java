/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NoPruningPruningStrategyTest
{
    @Test
    void shouldNotExcludeAnySegmentPages()
    {
        NoPruningPruningStrategy strategy = new NoPruningPruningStrategy();

        //when
        long indexToKeep = strategy.getIndexToKeep( null );

        //then
        assertEquals( -1, indexToKeep );
    }
}
