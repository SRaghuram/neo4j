/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.builtinprocs;

import java.time.ZoneId;

import org.neo4j.helpers.SocketAddress;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;

public class ListConnectionResult
{
    public final String connectionId;
    public final String connectTime;
    public final String connector;
    public final String username;
    public final String userAgent;
    public final String serverAddress;
    public final String clientAddress;

    ListConnectionResult( TrackedNetworkConnection connection, ZoneId timeZone )
    {
        connectionId = connection.id();
        connectTime = ProceduresTimeFormatHelper.formatTime( connection.connectTime(), timeZone );
        connector = connection.connector();
        username = connection.username();
        userAgent = connection.userAgent();
        serverAddress = SocketAddress.format( connection.serverAddress() );
        clientAddress = SocketAddress.format( connection.clientAddress() );
    }
}
