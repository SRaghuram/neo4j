/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;

public class GateEvent
{
    private final boolean isSuccess;

    private GateEvent( boolean isSuccess )
    {
        this.isSuccess = isSuccess;
    }

    private static GateEvent success = new GateEvent( true );
    private static GateEvent failure = new GateEvent( false );

    public static GateEvent getSuccess()
    {
        return success;
    }

    public static GateEvent getFailure()
    {
        return failure;
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
        GateEvent that = (GateEvent) o;
        return isSuccess == that.isSuccess;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( isSuccess );
    }
}
