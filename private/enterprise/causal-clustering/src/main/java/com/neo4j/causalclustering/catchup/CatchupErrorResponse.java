/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.util.Objects;

import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.util.Preconditions.checkArgument;

public class CatchupErrorResponse
{
    private final CatchupResult status;
    private final String message;

    public CatchupErrorResponse( CatchupResult status, String message )
    {
        checkArgument( SUCCESS_END_OF_STREAM != status, "Catchup error cannot contain successful status: " + status );
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
        if ( o == null || getClass() != o.getClass() )
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
