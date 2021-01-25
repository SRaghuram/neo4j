/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication.session;

import static java.lang.String.format;

/** Uniquely identifies an operation as performed under a global session. */
public class LocalOperationId
{
    private final long localSessionId;
    private final long sequenceNumber;

    public LocalOperationId( long localSessionId, long sequenceNumber )
    {
        this.localSessionId = localSessionId;
        this.sequenceNumber = sequenceNumber;
    }

    public long localSessionId()
    {
        return localSessionId;
    }

    public long sequenceNumber()
    {
        return sequenceNumber;
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

        LocalOperationId that = (LocalOperationId) o;

        if ( localSessionId != that.localSessionId )
        {
            return false;
        }
        return sequenceNumber == that.sequenceNumber;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (localSessionId ^ (localSessionId >>> 32));
        result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return format( "LocalOperationId{localSessionId=%d, sequenceNumber=%d}", localSessionId, sequenceNumber );
    }
}
