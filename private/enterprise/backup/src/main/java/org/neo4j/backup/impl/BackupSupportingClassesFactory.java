/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.OutputStream;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.catchup.CatchupClientBuilder;
import org.neo4j.causalclustering.catchup.CatchupClientFactory;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.CatchupProtocolFeatures;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * The dependencies for the backup strategies require a valid configuration for initialisation.
 * By having this factory we can wait until the configuration has been loaded and the provide all the classes required
 * for backups that are dependant on the config.
 */
public class BackupSupportingClassesFactory
{
    protected final LogProvider logProvider;
    protected final Clock clock;
    protected final Monitors monitors;
    protected final FileSystemAbstraction fileSystemAbstraction;
    protected final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    protected final OutputStream logDestination;
    protected final OutsideWorld outsideWorld;
    private final JobScheduler jobScheduler;

    protected BackupSupportingClassesFactory( BackupModule backupModule )
    {
        this.logProvider = backupModule.getLogProvider();
        this.clock = backupModule.getClock();
        this.monitors = backupModule.getMonitors();
        this.fileSystemAbstraction = backupModule.getFileSystemAbstraction();
        this.transactionLogCatchUpFactory = backupModule.getTransactionLogCatchUpFactory();
        this.jobScheduler = backupModule.jobScheduler();
        this.logDestination = backupModule.getOutsideWorld().outStream();
        this.outsideWorld = backupModule.getOutsideWorld();
    }

    /**
     * Resolves all the backing solutions used for performing various backups while sharing key classes and
     * configuration.
     *
     * @return grouping of instances used for performing backups
     */
    BackupSupportingClasses createSupportingClasses( OnlineBackupContext context )
    {
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld ) );
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

        TxPullClient txPullClient = new TxPullClient( catchUpClient, databaseName, () -> monitors );
        ExponentialBackoffStrategy backOffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );
        StoreCopyClient storeCopyClient = new StoreCopyClient( catchUpClient, databaseName, () -> monitors, logProvider, backOffStrategy );

        RemoteStore remoteStore = new RemoteStore( logProvider, fileSystemAbstraction, pageCache, storeCopyClient,
                txPullClient, transactionLogCatchUpFactory, config, () -> monitors );

        return backupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    protected PipelineWrapper createPipelineWrapper( Config config )
    {
        return new VoidPipelineWrapperFactory().forClient( config, null, logProvider, OnlineBackupSettings.ssl_policy );
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
                .scheduler( jobScheduler )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .clock( clock )
                .debugLogProvider( logProvider )
                .userLogProvider( logProvider )
                .build();
    }

    private ApplicationSupportedProtocols getSupportedCatchupProtocols( OnlineBackupRequiredArguments arguments,
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
