/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupClientFactory;
import org.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

public class TxPullClient
{
    private final CatchupClientFactory catchUpClient;
    private final String databaseName;
    private final Supplier<Monitors> monitors;

    private PullRequestMonitor pullRequestMonitor = new PullRequestMonitor();

    public TxPullClient( CatchupClientFactory catchUpClient, String databaseName, Supplier<Monitors> monitors )
    {
        this.catchUpClient = catchUpClient;
        this.databaseName = databaseName;
        this.monitors = monitors;
    }

    public TxPullResult pullTransactions( AdvertisedSocketAddress fromAddress, StoreId storeId, long previousTxId,
                                                 TxPullResponseListener txPullResponseListener ) throws Exception
    {
        CatchupResponseAdaptor<TxPullResult> responseHandler = new CatchupResponseAdaptor<TxPullResult>()
        {
            private long lastTxIdReceived = previousTxId;

            @Override
            public void onTxPullResponse( CompletableFuture<TxPullResult> signal, TxPullResponse response )
            {
                this.lastTxIdReceived = response.tx().getCommitEntry().getTxId();
                txPullResponseListener.onTxReceived( response );
            }

            @Override
            public void onTxStreamFinishedResponse( CompletableFuture<TxPullResult> signal, TxStreamFinishedResponse response )
            {
                signal.complete( new TxPullResult( response.status(), lastTxIdReceived ) );
            }
        };

        pullRequestMonitor.get().txPullRequest( previousTxId );

        return catchUpClient.getClient( fromAddress )
                .v1( c -> c.pullTransactions( storeId, previousTxId ) )
                .v2( c -> c.pullTransactions( storeId, previousTxId, databaseName ) )
                .withResponseHandler( responseHandler )
                .request()
                .get();
    }

    private class PullRequestMonitor
    {
        private org.neo4j.causalclustering.catchup.tx.PullRequestMonitor pullRequestMonitor;

        private org.neo4j.causalclustering.catchup.tx.PullRequestMonitor get()
        {
            if ( pullRequestMonitor == null )
            {
                pullRequestMonitor = monitors.get().newMonitor( org.neo4j.causalclustering.catchup.tx.PullRequestMonitor.class );
            }
            return pullRequestMonitor;
        }
    }
}
