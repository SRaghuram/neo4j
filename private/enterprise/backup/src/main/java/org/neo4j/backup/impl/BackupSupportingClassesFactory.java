/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.handlers.PipelineWrapper;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol.CatchupProtocolFeatures;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

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
    private final JobScheduler jobScheduler;
    private final StorageEngineFactory storageEngineFactory;

    public BackupSupportingClassesFactory( StorageEngineFactory storageEngineFactory, FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider,
            Monitors monitors )
    {
        this.logProvider = logProvider;
        this.clock = Clock.systemUTC();
        this.monitors = monitors;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        this.jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
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
        PageCache pageCache = createPageCache( fileSystemAbstraction, context.getConfig(), jobScheduler );
        return new BackupSupportingClasses(
                backupDelegatorFromConfig( pageCache, context.getConfig(), context.getRequiredArguments() ),
                pageCache,
                Arrays.asList( pageCache, jobScheduler ) );
    }

    private BackupDelegator backupDelegatorFromConfig( PageCache pageCache, Config config, OnlineBackupRequiredArguments arguments )
    {
        CatchupClientFactory catchUpClient = catchUpClient( config, arguments );
        String databaseName = arguments.getDatabaseName().orElse( DEFAULT_DATABASE_NAME );

        TxPullClient txPullClient = new TxPullClient( catchUpClient, databaseName, () -> monitors, logProvider );
        ExponentialBackoffStrategy backOffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchUpClient, databaseName, () -> monitors, logProvider, backOffStrategy );

        RemoteStore remoteStore = new RemoteStore( logProvider, fileSystemAbstraction, pageCache, storeCopyClient,
                txPullClient, transactionLogCatchUpFactory, config, monitors, storageEngineFactory );

        return backupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    @VisibleForTesting
    protected PipelineWrapper createPipelineWrapper( Config config )
    {
        SecurePipelineFactory factory = new SecurePipelineFactory();
        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        return factory.forClient( config, OnlineBackupSettings.ssl_policy, sslPolicyLoader );
    }

    private CatchupClientFactory catchUpClient( Config config, OnlineBackupRequiredArguments arguments )
    {
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = getSupportedCatchupProtocols( arguments, supportedProtocolCreator );

        return CatchupClientBuilder.builder()
                .defaultDatabaseName( config.get( GraphDatabaseSettings.active_database ) )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( new NettyPipelineBuilderFactory( createPipelineWrapper( config ) ) )
                .inactivityTimeout( config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ) )
                .scheduler( jobScheduler )
                .bootstrapConfig( BootstrapConfiguration.clientConfig( config ) )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .clock( clock )
                .debugLogProvider( logProvider )
                .userLogProvider( logProvider ).build();
    }

    private static ApplicationSupportedProtocols getSupportedCatchupProtocols( OnlineBackupRequiredArguments arguments,
            SupportedProtocolCreator supportedProtocolCreator )
    {
        ApplicationSupportedProtocols catchupProtocol;
        if ( arguments.getDatabaseName().isPresent() )
        {
            catchupProtocol = supportedProtocolCreator.getMinimumCatchupProtocols( CatchupProtocolFeatures.SUPPORTS_MULTIPLE_DATABASES_FROM_VERSION );
        }
        else
        {
            catchupProtocol = supportedProtocolCreator.getAllCatchupProtocols();
        }
        return catchupProtocol;
    }

    private static BackupDelegator backupDelegator(
            RemoteStore remoteStore, CatchupClientFactory catchUpClient, StoreCopyClient storeCopyClient )
    {
        return new BackupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystemAbstraction, Config config, JobScheduler jobScheduler )
    {
        return ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction, config, jobScheduler );
    }
}
