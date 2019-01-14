/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NoPruningPruningStrategyTest
{
    @Test
    public void shouldNotExcludeAnySegmentPages()
    {
         NoPruningPruningStrategy strategy = new NoPruningPruningStrategy();

        //when
        long indexToKeep = strategy.getIndexToKeep( null );

        //then
        assertEquals( -1, indexToKeep );
    }
}
