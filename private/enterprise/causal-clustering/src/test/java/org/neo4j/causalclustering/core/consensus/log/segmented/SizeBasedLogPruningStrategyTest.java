/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SizeBasedLogPruningStrategyTest extends PruningStrategyTest
{
    @Test
    public void indexToKeepTest()
    {
        // given
        int segmentFilesCount = 14;
        int bytesToKeep = 6;
        int expectedIndex = segmentFilesCount - bytesToKeep;

        files = createSegmentFiles( segmentFilesCount );

        SizeBasedLogPruningStrategy sizeBasedLogPruningStrategy = new SizeBasedLogPruningStrategy( bytesToKeep );

        // when
        long indexToKeep = sizeBasedLogPruningStrategy.getIndexToKeep( segments );

        // then
        assertEquals( expectedIndex, indexToKeep );
    }
}
