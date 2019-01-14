/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

public class StoreCopyFinishedResponse
{
    public enum Status
    {
        SUCCESS,
        E_STORE_ID_MISMATCH,
        E_TOO_FAR_BEHIND,
        E_UNKNOWN,
        E_DATABASE_UNKNOWN
    }

    private final Status status;

    public StoreCopyFinishedResponse( Status status )
    {
        this.status = status;
    }

    public Status status()
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
        StoreCopyFinishedResponse that = (StoreCopyFinishedResponse) o;
        return status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status );
    }
}
