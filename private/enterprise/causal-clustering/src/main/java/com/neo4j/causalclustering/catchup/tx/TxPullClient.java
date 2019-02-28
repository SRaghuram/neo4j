/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.identity.StoreId;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class TxPullClient
{
    private final CatchupClientFactory catchUpClient;
    private final String databaseName;
    private final Supplier<Monitors> monitors;
    private final LogProvider logProvider;

    private PullRequestMonitor pullRequestMonitor = new PullRequestMonitor();

    public TxPullClient( CatchupClientFactory catchUpClient, String databaseName, Supplier<Monitors> monitors, LogProvider logProvider )
    {
        this.catchUpClient = catchUpClient;
        this.databaseName = databaseName;
        this.monitors = monitors;
        this.logProvider = logProvider;
    }

    public TxStreamFinishedResponse pullTransactions( AdvertisedSocketAddress fromAddress, StoreId storeId, long previousTxId,
                                                 TxPullResponseListener txPullResponseListener ) throws Exception
    {
        CatchupResponseAdaptor<TxStreamFinishedResponse> responseHandler = new CatchupResponseAdaptor<TxStreamFinishedResponse>()
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
                .v1( c -> c.pullTransactions( storeId, previousTxId ) )
                .v2( c -> c.pullTransactions( storeId, previousTxId, databaseName ) )
                .v3( c -> c.pullTransactions( storeId, previousTxId, databaseName ) )
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
