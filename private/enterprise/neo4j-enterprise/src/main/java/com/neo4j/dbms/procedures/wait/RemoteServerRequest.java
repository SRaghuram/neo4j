/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;

import java.util.concurrent.CompletableFuture;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

class RemoteServerRequest extends ServerRequest
{
    private final SocketAddress target;
    private final CatchupClientFactory catchupClientFactory;
    private final Log log;

    RemoteServerRequest( long systemTxId, ServerId serverId, SocketAddress socketAddress, SocketAddress target,
            CatchupClientFactory catchupClientFactory, NamedDatabaseId databaseId, Log log )
    {
        super( systemTxId, serverId, socketAddress, databaseId, log );
        this.target = target;
        this.catchupClientFactory = catchupClientFactory;
        this.log = log;
    }

    @Override
    public InfoResponse getInfo( NamedDatabaseId databaseId ) throws Exception
    {
        return catchupClientFactory.getClient( target, log )
                .v3( cli -> cli.getReconciledInfo( databaseId ) )
                .v4( cli -> cli.getReconciledInfo( databaseId ) )
                .v5( cli -> cli.getReconciledInfo( databaseId ) )
                .withResponseHandler( new CatchupResponseAdaptor<>()
                {
                    @Override
                    public void onInfo( CompletableFuture<InfoResponse> signal, InfoResponse response )
                    {
                        signal.complete( response );
                    }
                } ).request();
    }
}
