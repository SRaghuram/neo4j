/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.helper.TimeoutStrategy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.CopyOnWriteHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class CatchupComponentsService implements CatchupComponentsRepository, CatchupComponentsFactory
{
    private final ClusteredDatabaseManager<? extends ClusteredDatabaseContext> clusteredDatabaseManager;
    private final CatchupClientFactory catchupClient;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final TimeoutStrategy storeCopyBackoffStrategy;
    private final Monitors monitors;
    private final StorageEngineFactory storageEngineFactory;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final Config config;
    private final LogProvider logProvider;
    private final Map<String,DatabaseCatchupComponents> components;

    CatchupComponentsService( ClusteredDatabaseManager<? extends ClusteredDatabaseContext> clusteredDatabaseManager, CatchupClientFactory catchupClient,
            CopiedStoreRecovery copiedStoreRecovery, FileSystemAbstraction fileSystem, PageCache pageCache, Config config, LogProvider logProvider,
            Monitors monitors, StorageEngineFactory storageEngineFactory )
    {
        this.clusteredDatabaseManager = clusteredDatabaseManager;
        this.catchupClient = catchupClient;
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logProvider = logProvider;
        storeCopyBackoffStrategy = new ExponentialBackoffStrategy( 1,
                config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );
        this.monitors = monitors;
        this.storageEngineFactory = storageEngineFactory;
        this.components = new CopyOnWriteHashMap<>();
    }

    @Override
    public Optional<DatabaseCatchupComponents> componentsFor( String databaseName )
    {
        return clusteredDatabaseManager.getDatabaseContext( databaseName )
                .map( db -> components.computeIfAbsent( databaseName, ignored -> createPerDatabaseComponents( db ) ) );
    }

    @Override
    public DatabaseCatchupComponents createPerDatabaseComponents( ClusteredDatabaseContext clusteredDatabaseContext )
    {
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchupClient, clusteredDatabaseContext.databaseName(), () -> monitors,
                logProvider, storeCopyBackoffStrategy );
        TransactionLogCatchUpFactory transactionLogFactory = new TransactionLogCatchUpFactory();
        TxPullClient txPullClient = new TxPullClient( catchupClient, clusteredDatabaseContext.databaseName(), () -> monitors, logProvider );

        RemoteStore remoteStore = new RemoteStore( logProvider, fileSystem, pageCache,
                storeCopyClient, txPullClient, transactionLogFactory, config, monitors, storageEngineFactory );

        StoreCopyProcess storeCopy = new StoreCopyProcess( fileSystem, pageCache, clusteredDatabaseContext,
                copiedStoreRecovery, remoteStore, logProvider );

        return new DatabaseCatchupComponents( remoteStore, storeCopy );
    }
}
