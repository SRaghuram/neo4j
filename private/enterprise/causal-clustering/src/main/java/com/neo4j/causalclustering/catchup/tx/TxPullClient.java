/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

public class TxPullClient
{
    private final CatchupClientFactory catchUpClient;
    private final DatabaseId databaseId;
    private final Supplier<Monitors> monitors;
    private final LogProvider logProvider;

    private PullRequestMonitor pullRequestMonitor = new PullRequestMonitor();

    public TxPullClient( CatchupClientFactory catchUpClient, DatabaseId databaseId, Supplier<Monitors> monitors, LogProvider logProvider )
    {
        this.catchUpClient = catchUpClient;
        this.databaseId = databaseId;
        this.monitors = monitors;
        this.logProvider = logProvider;
    }

    public TxStreamFinishedResponse pullTransactions( SocketAddress fromAddress, StoreId storeId, long previousTxId,
                                                 TxPullResponseListener txPullResponseListener ) throws Exception
    {
        CatchupResponseAdaptor<TxStreamFinishedResponse> responseHandler = new CatchupResponseAdaptor<>()
        {
            @Override
            public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxPullResponse response )
            {
                txPullResponseListener.onTxReceived( response );
            }

            @Override
            public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
            {
                signal.complete( response );
            }
        };

        pullRequestMonitor.get().txPullRequest( previousTxId );

        Log log = logProvider.getLog( getClass() );
        return catchUpClient.getClient( fromAddress, log )
                .v3( c -> c.pullTransactions( storeId, previousTxId, databaseId ) )
                .withResponseHandler( responseHandler )
                .request();
    }

    private class PullRequestMonitor
    {
        private com.neo4j.causalclustering.catchup.tx.PullRequestMonitor pullRequestMonitor;

        private com.neo4j.causalclustering.catchup.tx.PullRequestMonitor get()
        {
            if ( pullRequestMonitor == null )
            {
                pullRequestMonitor = monitors.get().newMonitor( com.neo4j.causalclustering.catchup.tx.PullRequestMonitor.class );
            }
            return pullRequestMonitor;
        }
    }
}
