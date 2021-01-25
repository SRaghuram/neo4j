/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;

public class SwitchOverResponse implements ClientMessage
{
    static final int MESSAGE_CODE = 2;

    public static final SwitchOverResponse FAILURE = new SwitchOverResponse( StatusCode.FAILURE );

    private final StatusCode status;

    SwitchOverResponse( StatusCode status )
    {
        this.status = status;
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
    }

    public StatusCode status()
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
        SwitchOverResponse that = (SwitchOverResponse) o;
        return status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status );
    }

    @Override
    public String toString()
    {
        return "SwitchOverResponse{" + "status=" + status + '}';
    }
}
