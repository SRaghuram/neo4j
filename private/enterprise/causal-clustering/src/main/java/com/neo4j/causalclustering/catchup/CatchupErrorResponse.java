/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.util.Objects;

public class CatchupErrorResponse
{
    private final CatchupResult status;
    private final String message;

    public CatchupErrorResponse( CatchupResult status, String message )
    {
        this.status = status;
        this.message = message;
    }

    public CatchupResult status()
    {
        return status;
    }

    public String message()
    {
        return message;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof CatchupErrorResponse) )
        {
            return false;
        }
        CatchupErrorResponse that = (CatchupErrorResponse) o;
        return status == that.status && Objects.equals( message, that.message );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( status, message );
    }

    @Override
    public String toString()
    {
        return "CatchupErrorResponse{" + "status=" + status + ", message='" + message + '\'' + '}';
    }
}
