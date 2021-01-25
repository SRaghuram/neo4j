/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;

public class GateEvent
{
    private final boolean isSuccess;

    private GateEvent( boolean isSuccess )
    {
        this.isSuccess = isSuccess;
    }

    private static final GateEvent SUCCESS = new GateEvent( true );
    private static final GateEvent FAILURE = new GateEvent( false );

    public static GateEvent getSuccess()
    {
        return SUCCESS;
    }

    public static GateEvent getFailure()
    {
        return FAILURE;
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
