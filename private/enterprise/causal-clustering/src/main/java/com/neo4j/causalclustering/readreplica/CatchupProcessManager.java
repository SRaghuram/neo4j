/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.UpstreamStrategyBasedAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.DatabaseCatchupComponents;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.scheduler.Group;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * This class is responsible for aggregating a number of {@link CatchupPollingProcess} instances and pulling transactions for
 * each database present on this machine. These pull operations are issued on a fixed interval and take place in parallel.
 * <p>
 * If the necessary transactions are not remotely available then a fresh copy of the
 * entire store will be pulled down.
 *
 */
public class CatchupProcessManager extends SafeLifecycle
{
    public enum Timers implements TimerService.TimerName
    {
        TX_PULLER_TIMER
    }

    private final Suspendable servicesToStopOnStoreCopy;
    private final TopologyService topologyService;
    private final CatchupClientFactory catchupClient;
    private final UpstreamDatabaseStrategySelector selectionStrategyPipeline;
    private final TimerService timerService;
    private final long txPullIntervalMillis;
    private final LifeSupport txPulling;
    private final VersionContextSupplier versionContextSupplier;
    private final CommandIndexTracker commandIndexTracker;
    private final PageCursorTracerSupplier pageCursorTracerSupplier;
    private final Executor executor;
    private final LogProvider logProvider;
    private final Log log;
    private final Config config;
    private final CatchupComponentsRepository catchupComponents;
    private final ClusteredDatabaseManager<?> clusteredDatabaseManager;
    private final CatchupProcessFactory catchupProcessFactory;
    private final Health databaseHealth;

    private Map<String,CatchupPollingProcess> catchupProcesses;
    private volatile boolean isPanicked;
    private Timer timer;

    CatchupProcessManager( Executor executor, CatchupComponentsRepository catchupComponents,
            ClusteredDatabaseManager<?> clusteredDatabaseManager, Suspendable servicesToStopOnStoreCopy,
            Health databaseHealth, TopologyService topologyService, CatchupClientFactory catchUpClient,
            UpstreamDatabaseStrategySelector selectionStrategyPipeline, TimerService timerService, CommandIndexTracker commandIndexTracker,
            LogProvider logProvider, VersionContextSupplier versionContextSupplier, PageCursorTracerSupplier pageCursorTracerSupplier, Config config )
    {
        this( executor, catchupComponents, clusteredDatabaseManager, servicesToStopOnStoreCopy, databaseHealth, topologyService,
                catchUpClient, selectionStrategyPipeline, timerService, commandIndexTracker, null, logProvider,
                versionContextSupplier, pageCursorTracerSupplier, config );
    }

    CatchupProcessManager( Executor executor, CatchupComponentsRepository catchupComponents,
            ClusteredDatabaseManager<?> clusteredDatabaseManager, Suspendable servicesToStopOnStoreCopy,
            Health databaseHealth, TopologyService topologyService, CatchupClientFactory catchUpClient,
            UpstreamDatabaseStrategySelector selectionStrategyPipeline, TimerService timerService, CommandIndexTracker commandIndexTracker,
            CatchupProcessFactory catchupProcessFactory, LogProvider logProvider, VersionContextSupplier versionContextSupplier,
            PageCursorTracerSupplier pageCursorTracerSupplier, Config config )
    {
        this.logProvider = logProvider;
        this.log = logProvider.getLog( this.getClass() );
        this.versionContextSupplier = versionContextSupplier;
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
        this.config = config;

        this.commandIndexTracker = commandIndexTracker;
        this.timerService = timerService;
        this.executor = executor;
        this.catchupComponents = catchupComponents;
        this.clusteredDatabaseManager = clusteredDatabaseManager;
        this.servicesToStopOnStoreCopy = servicesToStopOnStoreCopy;
        this.databaseHealth = databaseHealth;
        this.topologyService = topologyService;
        this.catchupClient = catchUpClient;
        this.selectionStrategyPipeline = selectionStrategyPipeline;
        this.txPullIntervalMillis = config.get( CausalClusteringSettings.pull_interval ).toMillis();
        this.txPulling = new LifeSupport();
        this.catchupProcessFactory = catchupProcessFactory == null ? this::createCatchupProcess : catchupProcessFactory;
        this.isPanicked = false;
    }

    @Override
    public void start0() throws Exception
    {
        log.info( "Starting " + this.getClass().getSimpleName() );
        catchupProcesses = clusteredDatabaseManager.registeredDatabases().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> catchupProcessFactory.create( e.getValue() ) ) );
        txPulling.start();
        initTimer();

        for ( CatchupPollingProcess catchupProcess : catchupProcesses.values() )
        {
            waitForUpToDateStore( catchupProcess );
        }
    }

    private void waitForUpToDateStore( CatchupPollingProcess catchupProcess ) throws InterruptedException, ExecutionException
    {
        boolean upToDate = false;
        do
        {
            try
            {
                upToDate = catchupProcess.upToDateFuture().get( 1, MINUTES );
            }
            catch ( TimeoutException e )
            {
                log.warn( "Waiting for up-to-date store. State: " + catchupProcess.describeState() );
            }
        }
        while ( !upToDate );
    }

    @Override
    public void stop0()
    {
        log.info( "Shutting down " + this.getClass().getSimpleName() );
        timer.kill( Timer.CancelMode.SYNC_WAIT );
        txPulling.stop();
    }

    public synchronized void panic( Throwable e )
    {
        log.error( "Unexpected issue in catchup process. No more catchup requests will be scheduled.", e );
        databaseHealth.panic( e );
        isPanicked = true;
    }

    private CatchupPollingProcess createCatchupProcess( ClusteredDatabaseContext databaseContext )
    {
        DatabaseCatchupComponents dbCatchupComponents = catchupComponents.componentsFor( databaseContext.databaseName() )
                .orElseThrow( () -> new IllegalArgumentException(
                        String.format( "No StoreCopyProcess instance exists for database %s.", databaseContext.databaseName() ) ) );

        //TODO: We can do better than this. Core already exposes its commit process. Why not RR.
        Supplier<TransactionCommitProcess> writableCommitProcess = () -> new TransactionRepresentationCommitProcess(
                databaseContext.database().getDependencyResolver().resolveDependency( TransactionAppender.class ),
                databaseContext.database().getDependencyResolver().resolveDependency( StorageEngine.class ) );

        int maxBatchSize = config.get( CausalClusteringSettings.read_replica_transaction_applier_batch_size );
        BatchingTxApplier batchingTxApplier = new BatchingTxApplier(
                maxBatchSize, () -> databaseContext.database().getDependencyResolver().resolveDependency( TransactionIdStore.class ),
                writableCommitProcess, databaseContext.monitors(), pageCursorTracerSupplier, versionContextSupplier, commandIndexTracker, logProvider );

        CatchupPollingProcess catchupProcess = new CatchupPollingProcess( executor, databaseContext.databaseName(), clusteredDatabaseManager,
                servicesToStopOnStoreCopy, catchupClient, batchingTxApplier, databaseContext.monitors(), dbCatchupComponents.storeCopyProcess(), logProvider,
                this::panic, new UpstreamStrategyBasedAddressProvider( topologyService, selectionStrategyPipeline ) );

        databaseContext.dependencies().satisfyDependencies( catchupProcess );
        txPulling.add( batchingTxApplier );
        txPulling.add( catchupProcess );
        return catchupProcess;
    }

    /**
     * Time to catchup, thrusters to maximum!
     */
    private void onTimeout() throws Exception
    {
        CompletableFuture[] catchups = catchupProcesses.values().stream().map( CatchupPollingProcess::tick ).toArray( CompletableFuture[]::new );
        CompletableFuture.allOf( catchups ).get();

        if ( !isPanicked )
        {
            timer.reset();
        }
    }

    @VisibleForTesting
    void setCatchupProcesses( Map<String, CatchupPollingProcess> catchupProcesses )
    {
        this.catchupProcesses = catchupProcesses;
    }

    void initTimer()
    {
        if ( timer == null )
        {
            timer = timerService.create( TX_PULLER_TIMER, Group.PULL_UPDATES, timeout -> onTimeout() );
            timer.set( fixedTimeout( txPullIntervalMillis, TimeUnit.MILLISECONDS ) );
        }
    }

    @FunctionalInterface
    interface CatchupProcessFactory
    {
        CatchupPollingProcess create( ClusteredDatabaseContext clusteredDatabaseContext );
    }
}
