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
import java.time.Duration;
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

import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

/**
 * Entry point for remote store related RPC.
 */
public class RemoteStore
{
    /**
     * Represents the minimal transaction ID that can be committed by the user.
     */
    private static final long MIN_COMMITTED_TRANSACTION_ID = BASE_TX_ID + 1;

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
            StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( destinationLayout.databaseDirectory(), fs, monitors );
            long lastCheckPointedTxId = storeCopyClient.copyStoreFiles( addressProvider, expectedStoreId, streamToDiskProvider,
                    this::perRequestTerminationCondition, destinationLayout.databaseDirectory() );

            CatchupResult catchupResult = pullTransactions( addressProvider.primary(), expectedStoreId, destinationLayout,
                    lastCheckPointedTxId, true, true, rotateTransactionsManually );
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
        // adjust the given transaction ID so that it represents a valid ID which can be received in the stream of transactions
        fromTxId = Math.max( fromTxId, MIN_COMMITTED_TRANSACTION_ID );

        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( databaseLayout, fs, pageCache, config,
                logProvider, storageEngineFactory, fromTxId, asPartOfStoreCopy, keepTxLogsInStoreDir, rotateTransactionsManually ) )
        {
            log.info( "Pulling transactions from %s starting with txId: %d", from, fromTxId );

            storeCopyClientMonitor.startReceivingTransactions( fromTxId );
            long previousTxId = fromTxId - 1; // pull transactions request needs to contain the previous ID
            TxStreamFinishedResponse result = txPullClient.pullTransactions( from, expectedStoreId, previousTxId, writer );
            storeCopyClientMonitor.finishReceivingTransactions( result.lastTxId() );

            return result.status();
        }
        catch ( Exception e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    public StoreId getStoreId( AdvertisedSocketAddress from ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( from );
    }

    private TerminationCondition perRequestTerminationCondition()
    {
        Duration maxRetryTime = config.get( CausalClusteringSettings.store_copy_max_retry_time_per_request );
        return new MaximumTotalTime( maxRetryTime.getSeconds(), TimeUnit.SECONDS );
    }
}
