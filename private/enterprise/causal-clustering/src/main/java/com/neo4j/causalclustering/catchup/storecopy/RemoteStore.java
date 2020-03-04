/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.core.CausalClusteringSettings;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.storecopy.TxPullRequestContext.createContextFromCatchingUp;
import static com.neo4j.causalclustering.catchup.storecopy.TxPullRequestContext.createContextFromStoreCopy;
import static com.neo4j.causalclustering.catchup.storecopy.TxPuller.createTxPuller;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

/**
 * Entry point for remote store related RPC.
 */
public class RemoteStore
{
    private final Log log;
    private final Monitors monitors;
    private final Config config;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final StoreCopyClient storeCopyClient;
    private final TxPullClient txPullClient;
    private final TransactionLogCatchUpFactory transactionLogFactory;
    private final CommitStateHelper commitStateHelper;
    private final StoreCopyClientMonitor storeCopyClientMonitor;
    private final StorageEngineFactory storageEngineFactory;
    private final NamedDatabaseId namedDatabaseId;

    public RemoteStore( LogProvider logProvider, FileSystemAbstraction fs, PageCache pageCache, StoreCopyClient storeCopyClient, TxPullClient txPullClient,
            TransactionLogCatchUpFactory transactionLogFactory, Config config, Monitors monitors, StorageEngineFactory storageEngineFactory,
            NamedDatabaseId namedDatabaseId )
    {
        this.logProvider = logProvider;
        this.storeCopyClient = storeCopyClient;
        this.txPullClient = txPullClient;
        this.fs = fs;
        this.pageCache = pageCache;
        this.transactionLogFactory = transactionLogFactory;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
        this.monitors = monitors;
        this.storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        this.storageEngineFactory = storageEngineFactory;
        this.namedDatabaseId = namedDatabaseId;
        this.commitStateHelper = new CommitStateHelper( pageCache, fs, config, storageEngineFactory );
    }

    public void tryCatchingUp( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, DatabaseLayout databaseLayout, boolean keepTxLogsInDir,
            boolean forceTransactionLogRotation )
            throws StoreCopyFailedException, IOException
    {
        CommitState commitState = commitStateHelper.getStoreState( databaseLayout, TRACER_SUPPLIER.get() );
        log.info( "Store commit state: " + commitState );

        TxPullRequestContext txPullRequestContext = createContextFromCatchingUp( expectedStoreId, commitState );
        pullTransactions( catchupAddressProvider, databaseLayout, txPullRequestContext, false, keepTxLogsInDir, forceTransactionLogRotation );
    }

    public void copy( CatchupAddressProvider addressProvider, StoreId expectedStoreId, DatabaseLayout destinationLayout, boolean rotateTransactionsManually )
            throws StoreCopyFailedException
    {
        StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( destinationLayout.databaseDirectory(), fs, monitors );
        RequiredTransactions requiredTransactions =
                storeCopyClient.copyStoreFiles( addressProvider, expectedStoreId, streamToDiskProvider, this::getTerminationCondition,
                        destinationLayout.databaseDirectory() );

        log.info( "Store files need to be recovered starting from: %s", requiredTransactions );

        TxPullRequestContext context = createContextFromStoreCopy( requiredTransactions, expectedStoreId );
        pullTransactions( addressProvider, destinationLayout, context, true, true, rotateTransactionsManually );
    }

    private MaximumTotalTime getTerminationCondition()
    {
        return new MaximumTotalTime( config.get( CausalClusteringSettings.store_copy_max_retry_time_per_request ) );
    }

    private void pullTransactions( CatchupAddressProvider catchupAddressProvider, DatabaseLayout databaseLayout, TxPullRequestContext context,
            boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir, boolean rotateTransactionsManually )
            throws StoreCopyFailedException
    {
        storeCopyClientMonitor.startReceivingTransactions( context.startTxIdExclusive() );
        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( databaseLayout, fs, pageCache, config, logProvider, storageEngineFactory,
                validInitialTxRange( context ), asPartOfStoreCopy, keepTxLogsInStoreDir, rotateTransactionsManually ) )
        {
            TxPuller txPuller = createTxPuller( catchupAddressProvider, logProvider, config, namedDatabaseId );

            txPuller.pullTransactions( context, writer, txPullClient );
            storeCopyClientMonitor.finishReceivingTransactions( writer.lastTx() );
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private LongRange validInitialTxRange( TxPullRequestContext context )
    {
        return LongRange.range( context.startTxIdExclusive() + 1, context.fallbackStartId().orElse( context.startTxIdExclusive() ) + 1 );
    }

    public StoreId getStoreId( SocketAddress from ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( from );
    }
}
