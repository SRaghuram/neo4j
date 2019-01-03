/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.identity.StoreId;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static com.neo4j.causalclustering.catchup.storecopy.TxPullRequestContext.createContextFromCatchingUp;
import static com.neo4j.causalclustering.catchup.storecopy.TxPullRequestContext.createContextFromStoreCopy;

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

    public RemoteStore( LogProvider logProvider, FileSystemAbstraction fs, PageCache pageCache, StoreCopyClient storeCopyClient, TxPullClient txPullClient,
            TransactionLogCatchUpFactory transactionLogFactory, Config config, Monitors monitors, StorageEngineFactory storageEngineFactory )
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
        this.commitStateHelper = new CommitStateHelper( pageCache, fs, config, storageEngineFactory );
    }

    public CatchupResult tryCatchingUp( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, DatabaseLayout databaseLayout,
            boolean keepTxLogsInDir,
            boolean forceTransactionLogRotation )
            throws StoreCopyFailedException, IOException
    {
        CommitState commitState = commitStateHelper.getStoreState( databaseLayout );
        log.info( "Store commit state: " + commitState );

        TxPullRequestContext txPullRequestContext = createContextFromCatchingUp( expectedStoreId, commitState );
        return pullTransactions( catchupAddressProvider, databaseLayout, txPullRequestContext, false, keepTxLogsInDir, forceTransactionLogRotation );
    }

    public void copy( CatchupAddressProvider addressProvider, StoreId expectedStoreId, DatabaseLayout destinationLayout, boolean rotateTransactionsManually )
            throws StoreCopyFailedException
    {
        RequiredTransactionRange requiredTransactionRange;
        StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( destinationLayout.databaseDirectory(), fs, monitors );
        requiredTransactionRange = storeCopyClient.copyStoreFiles( addressProvider, expectedStoreId, streamToDiskProvider, this::getTerminationCondition,
                destinationLayout.databaseDirectory() );

        log.info( "Store files need to be recovered starting from: %s", requiredTransactionRange );

        TxPullRequestContext context = createContextFromStoreCopy( requiredTransactionRange, expectedStoreId );
        CatchupResult catchupResult = pullTransactions( addressProvider, destinationLayout, context, true, true, rotateTransactionsManually );
        if ( catchupResult != SUCCESS_END_OF_STREAM )
        {
            throw new StoreCopyFailedException( "Failed to pull transactions: " + catchupResult );
        }
    }

    private MaximumTotalTime getTerminationCondition()
    {
        return new MaximumTotalTime( config.get( CausalClusteringSettings.store_copy_max_retry_time_per_request ).getSeconds(), TimeUnit.SECONDS );
    }

    private CatchupResult pullTransactions( CatchupAddressProvider catchupAddressProvider, DatabaseLayout databaseLayout, TxPullRequestContext context,
            boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir, boolean rotateTransactionsManually )
            throws StoreCopyFailedException
    {
        storeCopyClientMonitor.startReceivingTransactions( context.expectedFirstTxId().startTxId() );
        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( databaseLayout, fs, pageCache, config, logProvider, storageEngineFactory,
                context.expectedFirstTxId(), asPartOfStoreCopy, keepTxLogsInStoreDir, rotateTransactionsManually ) )
        {
            TxPullRequestExecutor executor = new TxPullRequestExecutor( catchupAddressProvider, logProvider, config );

            executor.pullTransactions( context, writer, txPullClient );
            storeCopyClientMonitor.finishReceivingTransactions( writer.lastTx() );
            return SUCCESS_END_OF_STREAM;
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    public StoreId getStoreId( AdvertisedSocketAddress from ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( from );
    }
}
