/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.values.AnyValue;

import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.utf8Value;

class ServerResponse
{

    private final ServerId serverId;

    private final SocketAddress address;
    private final WaitResponseState waitResponseState;
    private final String message;

    static ServerResponse caughtUp( ServerId serverId, SocketAddress address )
    {
        return new ServerResponse( serverId, address, WaitResponseState.CaughtUp, "caught up" );
    }

    static ServerResponse failed( ServerId serverId, SocketAddress address, String message )
    {
        return new ServerResponse( serverId, address, WaitResponseState.Failed, message );
    }

    public static ServerResponse incomplete( ServerId serverId, SocketAddress address )
    {
        return new ServerResponse( serverId, address, WaitResponseState.Incomplete, "server is still catching up" );
    }

    private ServerResponse( ServerId serverId, SocketAddress address, WaitResponseState waitResponseState, String message )
    {
        this.serverId = serverId;
        this.address = address;
        this.waitResponseState = waitResponseState;
        this.message = message;
    }

    public ServerId serverId()
    {
        return serverId;
    }

    public WaitResponseState state()
    {
        return waitResponseState;
    }

    public String message()
    {
        return message;
    }

    public SocketAddress address()
    {
        return address;
    }

    public AnyValue[] asValue( boolean successfulOutcome )
    {
        return new AnyValue[]{utf8Value( address.toString() ), utf8Value( waitResponseState.name() ), utf8Value( message ), booleanValue( successfulOutcome )};
    }

    @Override
    public String toString()
    {
        return "ServerResponse{" +
               "serverId=" + serverId +
               ", address=" + address +
               ", state=" + waitResponseState +
               ", message='" + message + '\'' +
               '}';
    }
}
