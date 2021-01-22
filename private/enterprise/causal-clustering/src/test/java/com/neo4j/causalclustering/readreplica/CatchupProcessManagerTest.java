/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.UpstreamStrategyBasedAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.core.consensus.schedule.CountingTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.readreplica.CatchupProcessFactory.CatchupProcessLifecycles;
import com.neo4j.causalclustering.readreplica.tx.AsyncTxApplier;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.Executor;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static com.neo4j.causalclustering.readreplica.CatchupProcessFactory.Timers.TX_PULLER_TIMER;
import static com.neo4j.dbms.ReplicatedDatabaseEventService.NO_EVENT_DISPATCH;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@ExtendWith( LifeExtension.class )
@TestInstance( PER_METHOD )
class CatchupProcessManagerTest
{
    @Inject
    LifeSupport life;

    private final CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );
    private final Panicker panicker = mock( Panicker.class );

    private final ReadReplicaDatabaseContext databaseContext = mock( ReadReplicaDatabaseContext.class );
    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final CountingTimerService timerService = new CountingTimerService( scheduler, nullLogProvider() );
    private final TopologyService topologyService = mock( TopologyService.class );

    private CatchupProcessFactory createProcessManager( CatchupPollingProcess catchupProcess )
    {
        var catchupClientFactory = mock( CatchupClientFactory.class );
        var upstreamStrategySelector = mock( UpstreamDatabaseStrategySelector.class );
        var commandIndexTracker = mock( CommandIndexTracker.class );
        var dependencies = new Dependencies();
        var database = mock( Database.class );
        when( databaseContext.dependencies() ).thenReturn( new Dependencies() );
        when( database.getDependencyResolver() ).thenReturn( dependencies );
        when( databaseContext.kernelDatabase() ).thenReturn( database );
        var asyncTxApplier = new AsyncTxApplier( scheduler, nullLogProvider() );

        var factory = createFactory( catchupProcess );
        return new CatchupProcessFactory( panicker, catchupComponents, topologyService, catchupClientFactory,
                upstreamStrategySelector, commandIndexTracker, nullLogProvider(), Config.defaults(),
                NO_EVENT_DISPATCH, PageCacheTracer.NULL, databaseContext, timerService, factory, asyncTxApplier );
    }

    private CatchupProcessFactory.CatchupProcessLifecyclesFactory createFactory( CatchupPollingProcess catchupProcess )
    {
        return ( CatchupComponentsRepository catchupComponents, CommandIndexTracker commandIndexTracker,
                ReplicatedDatabaseEventDispatch databaseEventDispatch, PageCacheTracer pageCacheTracer, Executor executor,
                CatchupClientFactory catchupClient, Panicker panicker, UpstreamStrategyBasedAddressProvider upstreamProvider,
                LogProvider logProvider, Config config, ReadReplicaDatabaseContext databaseContext ) ->
                new CatchupProcessLifecycles( catchupProcess );
    }

    @Test
    void shouldTickCatchupProcessOnTimeout()
    {
        // given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        createProcessManager(catchupProcess);
        life.add( createProcessManager( catchupProcess ) );

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( catchupProcess ).tick();
    }

    @Test
    void shouldNotRenewTheTimeoutOnPanic()
    {
        // given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        life.add( createProcessManager( catchupProcess ) );

        createProcessManager( catchupProcess );

        when( catchupProcess.isPanicked() ).thenReturn( true );
        Timer timer = spy( single( timerService.getTimers( TX_PULLER_TIMER ) ) );

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( timer, never() ).reset();
    }
}
