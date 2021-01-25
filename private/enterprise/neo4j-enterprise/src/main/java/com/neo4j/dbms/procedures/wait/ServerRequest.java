/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

abstract class ServerRequest
{
    protected final long systemTxId;
    protected final ServerId serverId;
    private final SocketAddress socketAddress;
    private final NamedDatabaseId databaseId;
    private final Log log;

    ServerRequest( long systemTxId, ServerId serverId, SocketAddress socketAddress, NamedDatabaseId databaseId, Log log )
    {
        this.systemTxId = systemTxId;
        this.serverId = serverId;
        this.socketAddress = socketAddress;
        this.databaseId = databaseId;
        this.log = log;
    }

    protected abstract InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception;

    public ServerResponse call()
    {
        try
        {
            var infoResponse = getInfo( databaseId );
            return handleResponse( databaseId, serverId, socketAddress, systemTxId, infoResponse );
        }
        catch ( Throwable t )
        {
            log.info( "Failed to get reconciliation info for " + databaseId + " from " + serverId, t );
        }
        return null;
    }

    private ServerResponse handleResponse( NamedDatabaseId databaseId, ServerId serverId, SocketAddress address, long systemTxId,
            InfoResponse info )
    {
        if ( info.reconciliationFailure().isPresent() )
        {
            String caughtUpMessage = info.reconciledId() >= systemTxId ? "Caught up but " : "Not caught up and ";
            return ServerResponse.failed( serverId, address,
                    caughtUpMessage + "has failure for " + databaseId + " Failure: " + info.reconciliationFailure().get() );
        }
        if ( info.reconciledId() >= systemTxId )
        {
            return ServerResponse.caughtUp( serverId, address );
        }
        return null;
    }

    public SocketAddress socketAddress()
    {
        return socketAddress;
    }
}
