/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SizeBasedLogPruningStrategyTest extends PruningStrategyTest
{
    @Test
    void indexToKeepTest()
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
        Assertions.assertEquals( expectedIndex, indexToKeep );
    }
}
