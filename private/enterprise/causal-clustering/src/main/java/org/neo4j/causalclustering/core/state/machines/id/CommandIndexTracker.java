/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

/**
 * Keeps track of the raft command index of last applied transaction.
 *
 * As soon as a transaction is successfully applied this will be updated to reflect that.
 */
public class CommandIndexTracker
{
    private volatile long appliedCommandIndex;

    public void setAppliedCommandIndex( long appliedCommandIndex )
    {
        this.appliedCommandIndex = appliedCommandIndex;
    }

    public long getAppliedCommandIndex()
    {
        return appliedCommandIndex;
    }
}
