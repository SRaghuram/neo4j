/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStream;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamProvider;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxPullResponseListener;
import com.neo4j.causalclustering.common.ClusterMonitors;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.storecopy.TerminationCondition.CONTINUE_INDEFINITELY;

class CatchupClientsWrapper
{
    private final File ignoredDestDir = new File( "." );
    private final StoreCopyClient storeCopyClient;
    private final TxPullClient txPullClient;
    private final CatchupAddressProvider catchupAddressProvider;

    private static final StoreFileStream NOOP_STREAM = new StoreFileStream()
    {
        @Override
        public void write( ByteBuf byteBuf )
        {
            // no-op
        }

        @Override
        public void close()
        {
            // no-op
        }
    };

    private static final StoreFileStreamProvider NOOP_STREAM_PROVIDER = ( destination, requiredAlignment ) -> NOOP_STREAM;
    private final TxPullResponseListener doNothing;

    private StoreId storeId;
    private long previousTxId;

    CatchupClientsWrapper( GlobalModule module, CatchupClientFactory catchupClientFactory, NamedDatabaseId databaseId, LogProvider logProvider,
            SocketAddress socketAddress )
    {
        var monitors = ClusterMonitors.create( module.getGlobalMonitors(), module.getGlobalDependencies() );
        var backupStrategy = new ConstantTimeTimeoutStrategy( 5, TimeUnit.SECONDS );
        this.storeCopyClient = new StoreCopyClient( catchupClientFactory, databaseId, () -> monitors, logProvider, backupStrategy );
        this.txPullClient = new TxPullClient( catchupClientFactory, databaseId, () -> monitors, logProvider );
        this.catchupAddressProvider = new CatchupAddressProvider.SingleAddressProvider( socketAddress );
        var log = logProvider.getLog( getClass() );
        this.doNothing = tx ->
        {
            log.info( "Tx received: " + tx );
        };
    }

    void storeCopy() throws CatchupAddressResolutionException, StoreCopyFailedException, StoreIdDownloadFailedException
    {
        storeId = storeCopyClient.fetchStoreId( catchupAddressProvider.primary( null ) );
        var copyResult = storeCopyClient.copyStoreFiles( catchupAddressProvider, storeId, NOOP_STREAM_PROVIDER, () -> CONTINUE_INDEFINITELY, ignoredDestDir );
        previousTxId = copyResult.startTxId() - 1;
    }

    void pullTransactions() throws Exception
    {
        txPullClient.pullTransactions( catchupAddressProvider.primary( null ), storeId, previousTxId, doNothing );
    }
}