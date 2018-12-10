/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
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

import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

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

    public RemoteStore( LogProvider logProvider, FileSystemAbstraction fs, PageCache pageCache, StoreCopyClient storeCopyClient, TxPullClient txPullClient,
            TransactionLogCatchUpFactory transactionLogFactory, Config config, Monitors monitors )
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
        this.commitStateHelper = new CommitStateHelper( pageCache, fs, config );
    }

    /**
     * Later stages of the startup process require at least one transaction to
     * figure out the mapping between the transaction log and the consensus log.
     *
     * If there are no transaction logs then we can pull from and including
     * the index which the metadata store points to. This would be the case
     * for example with a backup taken during an idle period of the system.
     *
     * However, if there are transaction logs then we want to find out where
     * they end and pull from there, excluding the last one so that we do not
     * get duplicate entries.
     */
    public CatchupResult tryCatchingUp( AdvertisedSocketAddress from, StoreId expectedStoreId, DatabaseLayout databaseLayout, boolean keepTxLogsInDir,
            boolean forceTransactionLogRotation )
            throws StoreCopyFailedException, IOException
    {
        CommitState commitState = commitStateHelper.getStoreState( databaseLayout );
        log.info( "Store commit state: " + commitState );

        if ( commitState.transactionLogIndex().isPresent() )
        {
            return pullTransactions( from, expectedStoreId, databaseLayout, commitState.transactionLogIndex().get() + 1, false, keepTxLogsInDir,
                    forceTransactionLogRotation );
        }
        else
        {
            CatchupResult catchupResult;
            if ( commitState.metaDataStoreIndex() == BASE_TX_ID )
            {
                return pullTransactions( from, expectedStoreId, databaseLayout, commitState.metaDataStoreIndex() + 1, false, keepTxLogsInDir,
                        forceTransactionLogRotation );
            }
            else
            {
                catchupResult = pullTransactions( from, expectedStoreId, databaseLayout, commitState.metaDataStoreIndex(), false, keepTxLogsInDir,
                        forceTransactionLogRotation );
                if ( catchupResult == E_TRANSACTION_PRUNED )
                {
                    return pullTransactions( from, expectedStoreId, databaseLayout, commitState.metaDataStoreIndex() + 1, false, keepTxLogsInDir,
                            forceTransactionLogRotation );
                }
            }
            return catchupResult;
        }
    }

    public void copy( CatchupAddressProvider addressProvider, StoreId expectedStoreId, DatabaseLayout destinationLayout, boolean rotateTransactionsManually )
            throws StoreCopyFailedException
    {
        try
        {
            long lastFlushedTxId;
            StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( destinationLayout.databaseDirectory(), fs, monitors );
            lastFlushedTxId = storeCopyClient.copyStoreFiles( addressProvider, expectedStoreId, streamToDiskProvider,
                        () -> new MaximumTotalTime( config.get( CausalClusteringSettings.store_copy_max_retry_time_per_request ).getSeconds(),
                                TimeUnit.SECONDS ), destinationLayout.databaseDirectory() );

            log.info( "Store files need to be recovered starting from: %d", lastFlushedTxId );

            CatchupResult catchupResult = pullTransactions( addressProvider.primary(), expectedStoreId, destinationLayout,
                    lastFlushedTxId, true, true, rotateTransactionsManually );
            if ( catchupResult != SUCCESS_END_OF_STREAM )
            {
                throw new StoreCopyFailedException( "Failed to pull transactions: " + catchupResult );
            }
        }
        catch ( CatchupAddressResolutionException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private CatchupResult pullTransactions( AdvertisedSocketAddress from, StoreId expectedStoreId, DatabaseLayout databaseLayout, long fromTxId,
            boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir, boolean rotateTransactionsManually )
            throws StoreCopyFailedException
    {
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingTransactions( fromTxId );
        long previousTxId = fromTxId - 1;
        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( databaseLayout, fs, pageCache, config,
                logProvider, fromTxId, asPartOfStoreCopy, keepTxLogsInStoreDir, rotateTransactionsManually ) )
        {
            log.info( "Pulling transactions from %s starting with txId: %d", from, fromTxId );
            CatchupResult lastStatus;

            TxStreamFinishedResponse result = txPullClient.pullTransactions( from, expectedStoreId, previousTxId, writer );
            lastStatus = result.status();
            previousTxId = result.lastTxId();

            return lastStatus;
        }
        catch ( Exception e )
        {
            throw new StoreCopyFailedException( e );
        }
        finally
        {
            storeCopyClientMonitor.finishReceivingTransactions( previousTxId );
        }
    }

    public StoreId getStoreId( AdvertisedSocketAddress from ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( from );
    }
}
