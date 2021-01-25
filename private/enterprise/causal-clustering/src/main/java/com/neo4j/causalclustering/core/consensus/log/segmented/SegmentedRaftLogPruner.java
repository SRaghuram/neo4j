/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

class SegmentedRaftLogPruner
{
    private final CoreLogPruningStrategy pruningStrategy;

    SegmentedRaftLogPruner( CoreLogPruningStrategy pruningStrategy )
    {
        this.pruningStrategy = pruningStrategy;
    }

    long getIndexToPruneFrom( long safeIndex, Segments segments )
    {
        return Math.min( safeIndex, pruningStrategy.getIndexToKeep( segments ) );
    }
}
