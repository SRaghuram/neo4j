/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;

import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.configuration.ssl.SslPolicyScope.BACKUP;

/**
 * The dependencies for the backup strategies require a valid configuration for initialisation.
 * By having this factory we can wait until the configuration has been loaded and the provide all the classes required
 * for backups that are dependant on the config.
 */
public class BackupSupportingClassesFactory
{
    private final LogProvider logProvider;
    private final Clock clock;
    private final Monitors monitors;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    private final StorageEngineFactory storageEngineFactory;

    public BackupSupportingClassesFactory( StorageEngineFactory storageEngineFactory, FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider,
            Monitors monitors )
    {
        this.logProvider = logProvider;
        this.clock = Clock.systemUTC();
        this.monitors = monitors;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        this.storageEngineFactory = storageEngineFactory;
    }

    /**
     * Resolves all the backing solutions used for performing various backups while sharing key classes and
     * configuration.
     *
     * @return grouping of instances used for performing backups
     */
    BackupSupportingClasses createSupportingClasses( OnlineBackupContext context )
    {
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        PageCache pageCache = createPageCache( fileSystemAbstraction, context.getConfig(), jobScheduler, pageCacheTracer );
        return new BackupSupportingClasses(
                backupDelegatorFromConfig( pageCache, context, jobScheduler, pageCacheTracer ),
                pageCache,
                pageCacheTracer,
                Arrays.asList( pageCache, jobScheduler ) );
    }

    private BackupDelegator backupDelegatorFromConfig( PageCache pageCache, OnlineBackupContext onlineBackupContext, JobScheduler jobScheduler,
            PageCacheTracer pageCacheTracer )
    {
        Config config = onlineBackupContext.getConfig();
        CatchupClientFactory catchUpClient = catchUpClient( onlineBackupContext, jobScheduler );

        Function<NamedDatabaseId,TxPullClient> txPullClient = databaseId -> new TxPullClient( catchUpClient, databaseId, () -> monitors, logProvider );
        ExponentialBackoffStrategy backOffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );

        Function<NamedDatabaseId,StoreCopyClient> storeCopyClient = databaseId ->
                new StoreCopyClient( catchUpClient, databaseId, () -> monitors, logProvider, backOffStrategy );

        Function<NamedDatabaseId,RemoteStore> remoteStore = databaseId -> new RemoteStore( logProvider, fileSystemAbstraction, pageCache,
                storeCopyClient.apply( databaseId ), txPullClient.apply( databaseId ), transactionLogCatchUpFactory, config, monitors, storageEngineFactory,
                databaseId, pageCacheTracer );

        return backupDelegator( remoteStore, storeCopyClient, catchUpClient, logProvider );
    }

    @VisibleForTesting
    protected NettyPipelineBuilderFactory createPipelineBuilderFactory( SslPolicy sslPolicy )
    {
        return new NettyPipelineBuilderFactory( sslPolicy );
    }

    private CatchupClientFactory catchUpClient( OnlineBackupContext onlineBackupContext, JobScheduler jobScheduler )
    {
        Config config = onlineBackupContext.getConfig();
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration();
        SslPolicy sslPolicy = loadSslPolicy( config );
        NettyPipelineBuilderFactory pipelineBuilderFactory = createPipelineBuilderFactory( sslPolicy );

        return CatchupClientBuilder.builder()
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( pipelineBuilderFactory )
                .inactivityTimeout( config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ) )
                .scheduler( jobScheduler )
                .bootstrapConfig( BootstrapConfiguration.clientConfig( config ) )
                .commandReader( storageEngineFactory.commandReaderFactory() )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .clock( clock )
                .debugLogProvider( logProvider ).build();
    }

    private SslPolicy loadSslPolicy( Config config )
    {
        var sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        return sslPolicyLoader.hasPolicyForSource( BACKUP ) ? sslPolicyLoader.getPolicy( BACKUP ) : null;
    }

    private static BackupDelegator backupDelegator( Function<NamedDatabaseId,RemoteStore> remoteStore,
            Function<NamedDatabaseId,StoreCopyClient> storeCopyClient, CatchupClientFactory catchUpClient, LogProvider logProvider )
    {
        return new BackupDelegator( remoteStore, storeCopyClient, catchUpClient, logProvider );
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystemAbstraction, Config config, JobScheduler jobScheduler,
            PageCacheTracer pageCacheTracer )
    {
        return ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction, config, jobScheduler, pageCacheTracer );
    }
}
