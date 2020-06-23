/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.configuration.CausalClusteringSettings;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.storecopy.TxPullRequestContext.createContextFromCatchingUp;
import static com.neo4j.causalclustering.catchup.storecopy.TxPullRequestContext.createContextFromStoreCopy;
import static com.neo4j.causalclustering.catchup.storecopy.TxPuller.createTxPuller;

/**
 * Entry point for remote store related RPC.
 */
public class RemoteStore
{
    private static final String STORE_STATE_READER_TAG = "storeStateReader";
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
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;

    public RemoteStore( LogProvider logProvider, FileSystemAbstraction fs, PageCache pageCache, StoreCopyClient storeCopyClient, TxPullClient txPullClient,
            TransactionLogCatchUpFactory transactionLogFactory, Config config, Monitors monitors, StorageEngineFactory storageEngineFactory,
            NamedDatabaseId namedDatabaseId, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
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
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
        this.commitStateHelper = new CommitStateHelper( pageCache, fs, config, storageEngineFactory );
    }

    public void tryCatchingUp( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, DatabaseLayout databaseLayout,
            boolean keepTxLogsInStoreDir ) throws StoreCopyFailedException, IOException
    {
        CommitState commitState = getCommitState( databaseLayout );
        log.info( "Store commit state: " + commitState );
        TxPullRequestContext txPullRequestContext = createContextFromCatchingUp( expectedStoreId, commitState );
        pullTransactions( catchupAddressProvider, databaseLayout, txPullRequestContext, false, keepTxLogsInStoreDir );
    }

    private CommitState getCommitState( DatabaseLayout databaseLayout ) throws IOException
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( STORE_STATE_READER_TAG ) )
        {
            return commitStateHelper.getStoreState( databaseLayout, cursorTracer );
        }
    }

    public void copy( CatchupAddressProvider addressProvider, StoreId expectedStoreId, DatabaseLayout destinationLayout ) throws StoreCopyFailedException
    {
        StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( destinationLayout.databaseDirectory().toFile(), fs, monitors );
        RequiredTransactions requiredTransactions =
                storeCopyClient.copyStoreFiles( addressProvider, expectedStoreId, streamToDiskProvider, this::getTerminationCondition,
                        destinationLayout.databaseDirectory().toFile() );

        log.info( "Store files need to be recovered starting from: %s", requiredTransactions );

        TxPullRequestContext context = createContextFromStoreCopy( requiredTransactions, expectedStoreId );
        pullTransactions( addressProvider, destinationLayout, context, true, true );
    }

    private MaximumTotalTime getTerminationCondition()
    {
        return new MaximumTotalTime( config.get( CausalClusteringSettings.store_copy_max_retry_time_per_request ) );
    }

    private void pullTransactions( CatchupAddressProvider catchupAddressProvider, DatabaseLayout databaseLayout, TxPullRequestContext context,
            boolean fullStoreCopy, boolean keepTxLogsInStoreDir ) throws StoreCopyFailedException
    {
        storeCopyClientMonitor.startReceivingTransactions( context.startTxIdExclusive() );
        try ( TransactionLogCatchUpWriter writer = transactionLogFactory.create( databaseLayout, fs, pageCache, config, logProvider, storageEngineFactory,
                validInitialTxRange( context ), fullStoreCopy, keepTxLogsInStoreDir, pageCacheTracer, memoryTracker ) )
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
