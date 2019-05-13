/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.DatabaseCatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.schedule.CountingTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helpers.FakeExecutor;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.test.FakeClockJobScheduler;

import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.single;

class CatchupProcessManagerTest
{
    private final CatchupClientFactory catchUpClient = mock( CatchupClientFactory.class );
    private final UpstreamDatabaseStrategySelector strategyPipeline = mock( UpstreamDatabaseStrategySelector.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );
    private final Health databaseHealth = mock( DatabaseHealth.class );
    private final PageCursorTracerSupplier pageCursorTracerSupplier = mock( PageCursorTracerSupplier.class );

    private final StubClusteredDatabaseManager databaseService = new StubClusteredDatabaseManager();
    private final List<DatabaseId> databaseIds = asList( new DatabaseId( "db1" ), new DatabaseId( "db2" ) );
    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final CountingTimerService timerService = new CountingTimerService( scheduler, NullLogProvider.getInstance() );

    private CatchupProcessManager catchupProcessManager;

    @BeforeEach
    void before()
    {
        //Mock the components of CatchupComponentsRepository
        databaseIds.forEach( name -> databaseService.registerDatabase( name, getMockDatabase( name ) ) );
        DatabaseCatchupComponents components = new DatabaseCatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) );

        //Wire these mocked components to the ServerModule mock
        when( catchupComponents.componentsFor( any( DatabaseId.class ) ) ).thenReturn( Optional.of( components ) );

        //Construct the manager under test
        catchupProcessManager = spy( new CatchupProcessManager( new FakeExecutor(), catchupComponents, databaseService,
                databaseHealth, topologyService, catchUpClient, strategyPipeline, timerService, new CommandIndexTracker(),
                NullLogProvider.getInstance(), pageCursorTracerSupplier, Config.defaults() ) );
    }

    private ClusteredDatabaseContext getMockDatabase( DatabaseId databaseId )
    {
        return databaseService.givenDatabaseWithConfig()
                .withDatabaseId( databaseId )
                .withDependencies( mock( Dependencies.class ) )
                .register();
    }

    @Test
    void shouldTickAllCatchupProcessesOnTimeout()
    {
        // given
        Map<DatabaseId,CatchupPollingProcess> catchupProcesses = databaseIds.stream()
                .collect( Collectors.toMap( Function.identity(), ignored -> mock( CatchupPollingProcess.class ) ) );
        catchupProcessManager.setCatchupProcesses( catchupProcesses );
        catchupProcessManager.initTimer();

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        catchupProcesses.values().forEach( client -> verify( client ).tick() );
    }

    @Test
    void shouldCreateCatchupProcessComponentsForEachDatabaseOnStart() throws Throwable
    {
        // given
        List<DatabaseId> startedCatchupProcs = new ArrayList<>();
        CatchupProcessManager.CatchupProcessFactory factory = db ->
        {
            startedCatchupProcs.add( db.databaseId() );
            CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
            when( catchupProcess.upToDateFuture() ).thenReturn( CompletableFuture.completedFuture( true ) );
            return catchupProcess;
        };
        catchupProcessManager = new CatchupProcessManager( new FakeExecutor(), catchupComponents, databaseService,
                databaseHealth, topologyService, catchUpClient, strategyPipeline, timerService, new CommandIndexTracker(),
                factory, NullLogProvider.getInstance(), pageCursorTracerSupplier, Config.defaults() );
        // when
        catchupProcessManager.init();
        catchupProcessManager.start();

        // then
        assertEquals( startedCatchupProcs, databaseIds );
    }

    @Test
    void shouldNotRenewTheTimeoutOnPanic()
    {
        // given
        Map<DatabaseId,CatchupPollingProcess> catchupProcesses = databaseIds.stream()
                .collect( Collectors.toMap( Function.identity(), ignored -> mock( CatchupPollingProcess.class ) ) );

        catchupProcessManager.initTimer();
        catchupProcessManager.setCatchupProcesses( catchupProcesses );
        catchupProcessManager.panic( new RuntimeException( "Don't panic Mr. Mainwaring!" ) );

        Timer timer = spy( single( timerService.getTimers( TX_PULLER_TIMER ) ) );

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( timer, never() ).reset();
    }
}
