/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

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
    private final Log log;
    private volatile long appliedCommandIndex;

    public CommandIndexTracker( LogProvider logProvider )
    {
        appliedCommandIndex = -1;
        log = logProvider.getLog( this.getClass() );
    }

    public synchronized void setAppliedCommandIndex( long appliedCommandIndex )
    {
        if ( this.appliedCommandIndex > appliedCommandIndex )
        {
            log.warn( format( "Warning, a command index tracker may only increase! Current index %d, attempted set to %d.",
                    this.appliedCommandIndex, appliedCommandIndex ) );
            return;
        }
        this.appliedCommandIndex = appliedCommandIndex;
    }

    public long getAppliedCommandIndex()
    {
        return appliedCommandIndex;
    }
}
