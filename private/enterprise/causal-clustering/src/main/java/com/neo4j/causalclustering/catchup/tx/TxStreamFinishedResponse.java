/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;

import java.util.Objects;

/**
 * Response type when executing a pulling transactions from a catchup server.
 */
public class TxStreamFinishedResponse
{
    private final CatchupResult status;
    private final long latestTxId;

    public TxStreamFinishedResponse( CatchupResult status, long latestTxId )
    {
        this.status = status;
        this.latestTxId = latestTxId;
    }

    public long lastTxId()
    {
        return latestTxId;
    }

    public CatchupResult status()
    {
        return status;
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
        TxStreamFinishedResponse that = (TxStreamFinishedResponse) o;
        return latestTxId == that.latestTxId && status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status, latestTxId );
    }

    @Override
    public String toString()
    {
        return "TxStreamFinishedResponse{" +
               "status=" + status +
               ", lastTxId=" + latestTxId +
               '}';
    }
}
