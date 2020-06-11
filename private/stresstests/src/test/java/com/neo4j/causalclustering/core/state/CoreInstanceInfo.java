/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;

import java.io.Serializable;

public class CoreInstanceInfo implements Serializable
{
    private final long term;
    private final long appendIndex;
    private final long commitIndex;
    private final long lastApplied;

    public CoreInstanceInfo( ExposedRaftState state, CommandApplicationProcess applicationProcess )
    {
        this.term = state.term();
        this.appendIndex = state.appendIndex();
        this.commitIndex = state.commitIndex();
        this.lastApplied = applicationProcess.lastApplied();
    }

    public long appendIndex()
    {
        return appendIndex;
    }

    public long appliedIndex()
    {
        return lastApplied;
    }

    @Override
    public String toString()
    {
        return "CoreInstanceInfo{" +
               "term=" + term +
               ", appendIndex=" + appendIndex +
               ", commitIndex=" + commitIndex +
               ", lastApplied=" + lastApplied +
               '}';
    }
}
