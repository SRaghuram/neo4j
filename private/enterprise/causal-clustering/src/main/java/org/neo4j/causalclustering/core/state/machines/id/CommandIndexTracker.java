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
 *
 * As raft logs indices can only monotonically increase with time, this tracker can also only monotonically increase.
 * Any attempt to decrease the most recently applied command index will be logged and ignored.
 */
public class CommandIndexTracker
{
    private volatile long appliedCommandIndex;

    public CommandIndexTracker()
    {
        this.appliedCommandIndex = -1;
    }

    public synchronized void registerAppliedCommandIndex( long appliedCommandIndex )
    {
        this.appliedCommandIndex = Math.max( this.appliedCommandIndex, appliedCommandIndex );
    }

    public long getAppliedCommandIndex()
    {
        return appliedCommandIndex;
    }
}
