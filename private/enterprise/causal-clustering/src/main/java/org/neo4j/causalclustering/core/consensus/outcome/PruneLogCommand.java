/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.outcome;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.logging.Log;

public class PruneLogCommand implements RaftLogCommand
{
    private final long pruneIndex;

    public PruneLogCommand( long pruneIndex )
    {
        this.pruneIndex = pruneIndex;
    }

    @Override
    public void dispatch( Handler handler )
    {
        handler.prune( pruneIndex );
    }

    @Override
    public void applyTo( RaftLog raftLog, Log log ) throws IOException
    {
        raftLog.prune( pruneIndex );
    }

    @Override
    public void applyTo( InFlightCache inFlightCache, Log log )
    {
        // only the actual log prunes
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        PruneLogCommand that = (PruneLogCommand) o;
        return pruneIndex == that.pruneIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( pruneIndex );
    }

    @Override
    public String toString()
    {
        return "PruneLogCommand{" +
               "pruneIndex=" + pruneIndex +
               '}';
    }
}
