/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import java.util.Objects;

public class LockResult
{
    private final LockStatus status;
    private final String message;

    public LockResult( LockStatus status )
    {
        this.status = status;
        this.message = null;
    }

    public LockResult( LockStatus status, String message )
    {
        this.status = status;
        this.message = message;
    }

    public LockStatus getStatus()
    {
        return status;
    }

    public String getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return "LockResult[" + status + ", " + message + "]";
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
        LockResult that = (LockResult) o;
        return Objects.equals( status, that.status ) &&
                Objects.equals( message, that.message );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status, message );
    }
}
