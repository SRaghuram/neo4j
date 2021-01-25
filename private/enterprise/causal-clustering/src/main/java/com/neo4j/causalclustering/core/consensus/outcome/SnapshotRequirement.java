/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

public class SnapshotRequirement
{
    private final long leaderPrevIndex;
    private final long localAppendIndex;

    SnapshotRequirement( long leaderPrevIndex, long localAppendIndex )
    {
        this.leaderPrevIndex = leaderPrevIndex;
        this.localAppendIndex = localAppendIndex;
    }

    public long leaderPrevIndex()
    {
        return leaderPrevIndex;
    }

    public long localAppendIndex()
    {
        return localAppendIndex;
    }

    @Override
    public String toString()
    {
        return "SnapshotRequirement{" + "leaderPrevIndex=" + leaderPrevIndex + ", localAppendIndex=" + localAppendIndex + '}';
    }
}
