/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.outcome;

import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.logging.Log;

public class TruncateLogCommand implements RaftLogCommand
{
    public final long fromIndex;

    public TruncateLogCommand( long fromIndex )
    {
        this.fromIndex = fromIndex;
    }

    @Override
    public void dispatch( Handler handler ) throws IOException
    {
        handler.truncate( fromIndex );
    }

    @Override
    public void applyTo( RaftLog raftLog, Log log ) throws IOException
    {
        raftLog.truncate( fromIndex );
    }

    @Override
    public void applyTo( InFlightCache inFlightCache, Log log )
    {
        log.debug( "Start truncating in-flight-map from index %d. Current map:%n%s", fromIndex, inFlightCache );
        inFlightCache.truncate( fromIndex );
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
        TruncateLogCommand that = (TruncateLogCommand) o;
        return fromIndex == that.fromIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fromIndex );
    }

    @Override
    public String toString()
    {
        return "TruncateLogCommand{" +
                "fromIndex=" + fromIndex +
                '}';
    }
}
