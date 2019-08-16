/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.schedule.CountingTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.CallingThreadExecutor;
import org.neo4j.test.FakeClockJobScheduler;

import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.single;

class CatchupProcessManagerTest
{
    private final CatchupClientFactory catchUpClient = mock( CatchupClientFactory.class );
    private final UpstreamDatabaseStrategySelector strategyPipeline = mock( UpstreamDatabaseStrategySelector.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );
    private final DatabasePanicker databasePanicker = mock( DatabasePanicker.class );
    private final PageCursorTracerSupplier pageCursorTracerSupplier = mock( PageCursorTracerSupplier.class );

    private final StubClusteredDatabaseManager databaseService = new StubClusteredDatabaseManager();
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DatabaseId databaseId = databaseIdRepository.getRaw( "db1" );
    private final ReadReplicaDatabaseContext databaseContext = mock( ReadReplicaDatabaseContext.class );
    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final CountingTimerService timerService = new CountingTimerService( scheduler, NullLogProvider.getInstance() );

    private CatchupProcessManager catchupProcessManager;

    @BeforeEach
    void before()
    {
        //Mock the components of CatchupComponentsRepository
        databaseService.registerDatabase( databaseId, getMockDatabase( databaseId ) );
        CatchupComponents components = new CatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) );

        //Wire these mocked components to the ServerModule mock
        when( catchupComponents.componentsFor( any( DatabaseId.class ) ) ).thenReturn( Optional.of( components ) );

        //Construct the manager under test
        catchupProcessManager = spy( new CatchupProcessManager( new CallingThreadExecutor(), catchupComponents, databaseContext,
                databasePanicker, topologyService, catchUpClient, strategyPipeline, timerService, new CommandIndexTracker(),
                NullLogProvider.getInstance(), pageCursorTracerSupplier, Config.defaults(), mock( ReplicatedDatabaseEventDispatch.class ) ) );
    }

    private ClusteredDatabaseContext getMockDatabase( DatabaseId databaseId )
    {
        return databaseService.givenDatabaseWithConfig()
                .withDatabaseId( databaseId )
                .withDependencies( mock( Dependencies.class ) )
                .register();
    }

    @Test
    void shouldTickCatchupProcessOnTimeout()
    {
        // given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        catchupProcessManager.setCatchupProcess( catchupProcess );
        catchupProcessManager.initTimer();

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

        catchupProcessManager.initTimer();
        catchupProcessManager.setCatchupProcess( catchupProcess );
        catchupProcessManager.panic( new RuntimeException( "Don't panic Mr. Mainwaring!" ) );

        Timer timer = spy( single( timerService.getTimers( TX_PULLER_TIMER ) ) );

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( timer, never() ).reset();
    }
}
