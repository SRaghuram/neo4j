/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import java.util.Objects;
import java.util.Optional;

public class InfoResponse
{
    private final long reconciledId;
    private final String message;

    private InfoResponse( long reconciledId, String message )
    {
        this.reconciledId = reconciledId;
        this.message = message;
    }

    public static InfoResponse create( long reconciledId, String reconciliationFailure )
    {
        return new InfoResponse( reconciledId, reconciliationFailure );
    }

    public long reconciledId()
    {
        return reconciledId;
    }

    public Optional<String> reconciliationFailure()
    {
        return Optional.ofNullable( message );
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
        InfoResponse that = (InfoResponse) o;
        return reconciledId == that.reconciledId &&
               Objects.equals( message, that.message );
    }

    @Override
    public String toString()
    {
        return "ReconciliationInfoResponse{" +
               "reconciledId=" + reconciledId +
               ", message='" + message + '\'' +
               '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( reconciledId, message );
    }
}
