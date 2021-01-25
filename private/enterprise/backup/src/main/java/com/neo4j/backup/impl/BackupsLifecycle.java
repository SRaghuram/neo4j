/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.configuration.CausalClusteringSettings;

import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.ssl.SslPolicyScope.BACKUP;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

/**
 * The dependencies for the backup strategies require a valid configuration for initialisation. By having this factory we can wait until the configuration has
 * been loaded and then provide all the classes required for backups that are dependant on the config.
 */
public class BackupsLifecycle implements AutoCloseable
{
    private final JobScheduler jobScheduler;
    private final PageCache pageCache;
    private final CatchupClientFactory catchupClientFactory;

    public static BackupsLifecycle startLifecycle( StorageEngineFactory storageEngineFactory,
                                                   FileSystemAbstraction fileSystemAbstraction,
                                                   LogProvider logProvider,
                                                   SystemNanoClock clock,
                                                   Config config,
                                                   PageCacheTracer pageCacheTracer )
    {
        var jobScheduler = createInitialisedScheduler( clock );
        var pageCache = createPageCache( fileSystemAbstraction, config, jobScheduler, pageCacheTracer );
        var catchupClientFactory = catchUpClient( config, jobScheduler, storageEngineFactory, clock, logProvider );
        var backupsLifecycle = new BackupsLifecycle( jobScheduler, pageCache, catchupClientFactory );
        backupsLifecycle.catchupClientFactory.init();
        backupsLifecycle.catchupClientFactory.start();
        return backupsLifecycle;
    }

    private BackupsLifecycle( JobScheduler initialisedScheduler, PageCache pageCache, CatchupClientFactory catchupClientFactory )
    {
        this.jobScheduler = initialisedScheduler;
        this.pageCache = pageCache;
        this.catchupClientFactory = catchupClientFactory;
    }

    public CatchupClientFactory getCatchupClientFactory()
    {
        return catchupClientFactory;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public JobScheduler getJobScheduler()
    {
        return jobScheduler;
    }

    public void close() throws Exception
    {
        catchupClientFactory.stop();
        catchupClientFactory.shutdown();
        IOUtils.closeAll( pageCache, jobScheduler );
    }

    private static CatchupClientFactory catchUpClient( Config config,
                                                       JobScheduler jobScheduler,
                                                       StorageEngineFactory storageEngineFactory,
                                                       SystemNanoClock clock,
                                                       LogProvider logProvider )
    {
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration();
        SslPolicy sslPolicy = loadSslPolicy( config, logProvider );
        NettyPipelineBuilderFactory pipelineBuilderFactory = new NettyPipelineBuilderFactory( sslPolicy );

        return CatchupClientBuilder
                .builder()
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( pipelineBuilderFactory )
                .inactivityTimeout( config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ) )
                .scheduler( jobScheduler )
                .config( config )
                .bootstrapConfig( BootstrapConfiguration.clientConfig( config ) )
                .commandReader( storageEngineFactory.commandReaderFactory() )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .clock( clock )
                .debugLogProvider( logProvider ).build();
    }

    private static SslPolicy loadSslPolicy( Config config, LogProvider logProvider )
    {
        var sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        return sslPolicyLoader.hasPolicyForSource( BACKUP ) ? sslPolicyLoader.getPolicy( BACKUP ) : null;
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystemAbstraction, Config config, JobScheduler jobScheduler,
                                              PageCacheTracer pageCacheTracer )
    {
        return ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction, config, jobScheduler, pageCacheTracer );
    }
}
