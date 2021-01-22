/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.UpstreamStrategyBasedAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.readreplica.tx.AsyncTxApplier;
import com.neo4j.causalclustering.readreplica.tx.BatchinTxApplierFactory;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static com.neo4j.causalclustering.readreplica.CatchupProcessFactory.Timers.TX_PULLER_TIMER;

/**
 * {@link CatchupPollingProcess} and {@link BatchingTxApplier} do the actual work of syncing a read replica with an upstream.
 * However they depend upon components of a kernel {@link Database} which do not exist until after that Database has started.
 *
 * Each instance of this factory only creates a single applier and polling process (wrapped in a {@link CatchupProcessLifecycles}).
 * Its job is to defer creation of those components till the point in a database's lifecycle when all necessary dependencies
 * exist. Subsequently, the factory will wrap the created components and delegate further lifecycle events to them.
 */
public class CatchupProcessFactory extends SafeLifecycle
{
    public enum Timers implements TimerService.TimerName
    {
        TX_PULLER_TIMER
    }

    private final CatchupClientFactory catchupClient;
    private final UpstreamStrategyBasedAddressProvider upstreamAddressProvider;
    private final CommandIndexTracker commandIndexTracker;
    private final Panicker panicker;
    private final LogProvider logProvider;
    private final Config config;
    private final CatchupComponentsRepository catchupComponents;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PageCacheTracer pageCacheTracer;
    private final AsyncTxApplier asyncTxApplier;
    private final ReadReplicaDatabaseContext databaseContext;
    private final TimerService timerService;
    private final long txPullIntervalMillis;
    private final CatchupProcessLifecyclesFactory lifecyclesFactory;
    private Timer timer;
    private CatchupProcessLifecycles wrapped;

    CatchupProcessFactory( Panicker panicker, CatchupComponentsRepository catchupComponents, TopologyService topologyService,
            CatchupClientFactory catchUpClient, UpstreamDatabaseStrategySelector selectionStrategyPipeline, CommandIndexTracker commandIndexTracker,
            LogProvider logProvider, Config config, ReplicatedDatabaseEventDispatch databaseEventDispatch, PageCacheTracer pageCacheTracer,
            AsyncTxApplier asyncTxApplier, ReadReplicaDatabaseContext databaseContext, TimerService timerService )
    {
        this( panicker, catchupComponents, topologyService, catchUpClient, selectionStrategyPipeline, commandIndexTracker, logProvider,
                config, databaseEventDispatch, pageCacheTracer, databaseContext, timerService, CatchupProcessFactory::create, asyncTxApplier );
    }

    @VisibleForTesting
    CatchupProcessFactory( Panicker panicker, CatchupComponentsRepository catchupComponents, TopologyService topologyService,
            CatchupClientFactory catchUpClient, UpstreamDatabaseStrategySelector selectionStrategyPipeline, CommandIndexTracker commandIndexTracker,
            LogProvider logProvider, Config config, ReplicatedDatabaseEventDispatch databaseEventDispatch, PageCacheTracer pageCacheTracer,
            ReadReplicaDatabaseContext databaseContext, TimerService timerService, CatchupProcessLifecyclesFactory lifecyclesFactory, AsyncTxApplier asyncTxApplier )
    {
        this.panicker = panicker;
        this.logProvider = logProvider;
        this.config = config;
        this.commandIndexTracker = commandIndexTracker;
        this.catchupComponents = catchupComponents;
        this.catchupClient = catchUpClient;
        this.databaseEventDispatch = databaseEventDispatch;
        this.pageCacheTracer = pageCacheTracer;
        this.asyncTxApplier = asyncTxApplier;
        this.databaseContext = databaseContext;
        this.timerService = timerService;
        this.txPullIntervalMillis = config.get( CausalClusteringSettings.pull_interval ).toMillis();
        this.lifecyclesFactory = lifecyclesFactory;
        this.upstreamAddressProvider = new UpstreamStrategyBasedAddressProvider( topologyService, selectionStrategyPipeline );
    }

    CatchupProcessLifecycles create( ReadReplicaDatabaseContext databaseContext )
    {
        return lifecyclesFactory.create( catchupComponents, commandIndexTracker, databaseEventDispatch, pageCacheTracer, executor, catchupClient,
                panicker, upstreamAddressProvider, logProvider, config, databaseContext );
    }

    private static CatchupProcessLifecycles create( CatchupComponentsRepository catchupComponents, CommandIndexTracker commandIndexTracker,
            ReplicatedDatabaseEventDispatch databaseEventDispatch, PageCacheTracer pageCacheTracer, Executor executor,
            CatchupClientFactory catchupClient, Panicker panicker, UpstreamStrategyBasedAddressProvider upstreamProvider,
            LogProvider logProvider, Config config, ReadReplicaDatabaseContext databaseContext )
    {
        var dbCatchupComponents = catchupComponents.componentsFor( databaseContext.databaseId() ).orElseThrow(
                () -> new IllegalArgumentException( String.format( "No StoreCopyProcess instance exists for database %s.", databaseContext.databaseId() ) ) );

        // TODO: We can do better than this. Core already exposes its commit process. Why not RR.
        Supplier<TransactionCommitProcess> writableCommitProcess = () -> new InternalTransactionCommitProcess(
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( TransactionAppender.class ),
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( StorageEngine.class ) );

        long applyBatchSize = ByteUnit.mebiBytes( config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_batch_size ) );
        long maxQueueSize = ByteUnit.mebiBytes( config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_max_queue_size ) );

        var batchingTxApplierFactory =
                new BatchinTxApplierFactory( databaseContext, commandIndexTracker, logProvider, databaseEventDispatch, pageCacheTracer, asyncTxApplier );

        CatchupPollingProcess catchupProcess =
                new CatchupPollingProcess( maxQueueSize, applyBatchSize, databaseContext, catchupClient, batchingTxApplierFactory, databaseEventDispatch,
                        dbCatchupComponents.storeCopyProcess(), logProvider, panicker, upstreamProvider );

        databaseContext.dependencies().satisfyDependency( catchupProcess ); //For ReadReplicaToggleProcedure

        return new CatchupProcessLifecycles( catchupProcess );
    }

    @Override
    public void start0()
    {
        if ( wrapped == null )
        {
            wrapped = create( databaseContext );
        }
        wrapped.start();
        initTimer();

    }

    @Override
    public void stop0()
    {
        wrapped.shutdown();
        timer.kill( Timer.CancelMode.SYNC_WAIT );
    }

    public Optional<CatchupProcessLifecycles> catchupProcessComponents()
    {
        return Optional.ofNullable( wrapped );
    }

    private void initTimer()
    {
        timer = timerService.create( TX_PULLER_TIMER, Group.PULL_UPDATES, timeout -> onTimeout() );
        timer.set( fixedTimeout( txPullIntervalMillis, TimeUnit.MILLISECONDS ) );
    }

    /**
     * Time to catchup, thrusters to maximum!
     */
    private void onTimeout() throws Exception
    {
        if ( wrapped == null )
        {
            return;
        }

        var catchupProcess = wrapped.catchupProcess();
        catchupProcess.tick();
        if ( !catchupProcess.isPanicked() )
        {
            timer.reset();
        }
    }

    static class CatchupProcessLifecycles extends LifeSupport
    {
        private final CatchupPollingProcess catchupProcess;

        CatchupProcessLifecycles( CatchupPollingProcess catchupProcess )
        {
            this.catchupProcess = catchupProcess;

            add( catchupProcess );
        }

        CatchupPollingProcess catchupProcess()
        {
            return catchupProcess;
        }
    }

    @FunctionalInterface
    interface CatchupProcessLifecyclesFactory
    {
        CatchupProcessLifecycles create( CatchupComponentsRepository catchupComponents, CommandIndexTracker commandIndexTracker,
                ReplicatedDatabaseEventDispatch databaseEventDispatch, PageCacheTracer pageCacheTracer, Executor executor,
                CatchupClientFactory catchupClient, Panicker panicker, UpstreamStrategyBasedAddressProvider upstreamProvider,
                LogProvider logProvider, Config config, ReadReplicaDatabaseContext databaseContext );
    }
}
