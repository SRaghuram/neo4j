/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class CatchupComponentsService implements CatchupComponentsRepository, CatchupComponentsFactory
{
    private final DatabaseService databaseService;
    private final CatchupClientFactory catchupClient;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final TimeoutStrategy storeCopyBackoffStrategy;
    private final Monitors monitors;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final Config config;
    private final LogProvider logProvider;
    private final Map<String, PerDatabaseCatchupComponents> components;

    CatchupComponentsService( DatabaseService databaseService, CatchupClientFactory catchupClient, CopiedStoreRecovery copiedStoreRecovery,
            FileSystemAbstraction fileSystem, PageCache pageCache, Config config, LogProvider logProvider, Monitors monitors )
    {
        this.databaseService = databaseService;
        this.catchupClient = catchupClient;
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logProvider = logProvider;
        storeCopyBackoffStrategy = new ExponentialBackoffStrategy( 1,
                config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );
        this.monitors = monitors;
        this.components = new CopyOnWriteHashMap<>();
    }

    @Override
    public Optional<PerDatabaseCatchupComponents> componentsFor( String databaseName )
    {
        return databaseService.get( databaseName )
                .map( db -> components.computeIfAbsent( databaseName, ignored -> createPerDatabaseComponents( db ) ) );
    }

    @Override
    public PerDatabaseCatchupComponents createPerDatabaseComponents( LocalDatabase localDatabase )
    {
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchupClient, localDatabase.databaseName(), () -> monitors,
                logProvider, storeCopyBackoffStrategy );
        TransactionLogCatchUpFactory transactionLogFactory = new TransactionLogCatchUpFactory();
        TxPullClient txPullClient = new TxPullClient( catchupClient, localDatabase.databaseName(), () -> monitors, logProvider );

        RemoteStore remoteStore = new RemoteStore( logProvider, fileSystem, pageCache,
                storeCopyClient, txPullClient, transactionLogFactory, config, monitors );

        StoreCopyProcess storeCopy = new StoreCopyProcess( fileSystem, pageCache, localDatabase,
                copiedStoreRecovery, remoteStore, logProvider );

        return new PerDatabaseCatchupComponents( remoteStore, storeCopy );
    }
}
