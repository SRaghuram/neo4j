/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.PerDatabaseCatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.common.LocalDatabase;
import com.neo4j.causalclustering.common.StubLocalDatabaseService;
import com.neo4j.causalclustering.core.consensus.schedule.CountingTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.Suspendable;
import com.neo4j.causalclustering.helpers.FakeExecutor;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.FakeClockJobScheduler;

import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.single;

public class CatchupProcessManagerTest
{
    private final CatchupClientFactory catchUpClient = mock( CatchupClientFactory.class );
    private final UpstreamDatabaseStrategySelector strategyPipeline = mock( UpstreamDatabaseStrategySelector.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final Suspendable startStopOnStoreCopy = mock( Suspendable.class );
    private final CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );
    private final DatabaseHealth databaseHealth = mock( DatabaseHealth.class );
    private final VersionContextSupplier versionContextSupplier = mock( VersionContextSupplier.class );
    private final PageCursorTracerSupplier pageCursorTracerSupplier = mock( PageCursorTracerSupplier.class );

    private final StubLocalDatabaseService databaseService = new StubLocalDatabaseService();
    private final List<String> databaseNames = asList( "db1", "db2" );
    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final CountingTimerService timerService = new CountingTimerService( scheduler, NullLogProvider.getInstance() );

    private CatchupProcessManager catchupProcessManager;

    @Before
    public void before()
    {
        //Mock the components of CatchupComponentsRepository
        databaseNames.forEach( name -> databaseService.registerDatabase( name, getMockDatabase( name ) ) );
        PerDatabaseCatchupComponents components = new PerDatabaseCatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) );

        //Wire these mocked components to the ServerModule mock
        when( catchupComponents.componentsFor( anyString() ) ).thenReturn( Optional.of( components ) );

        //Construct the manager under test
        catchupProcessManager = spy( new CatchupProcessManager( new FakeExecutor(), catchupComponents, databaseService, startStopOnStoreCopy,
                () -> databaseHealth, topologyService, catchUpClient, strategyPipeline, timerService, new CommandIndexTracker(),
                NullLogProvider.getInstance(), versionContextSupplier, pageCursorTracerSupplier, Config.defaults() ) );
    }

    private LocalDatabase getMockDatabase( String databaseName )
    {
        databaseService.givenDatabaseWithConfig()
                .withDatabaseName( databaseName )
                .withMonitors( new Monitors() )
                .withDependencies( mock( Dependencies.class ) )
                .register();
        return databaseService.get( databaseName ).get();
    }

    @Test
    public void shouldTickAllCatchupProcessesOnTimeout()
    {
        // given
        Map<String,CatchupPollingProcess> catchupProcesses = databaseNames.stream()
                .collect( Collectors.toMap( Function.identity(), ignored -> mock( CatchupPollingProcess.class ) ) );
        catchupProcessManager.setCatchupProcesses( catchupProcesses );
        catchupProcessManager.initTimer();

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        catchupProcesses.values().forEach( client -> verify( client ).tick() );
    }

    @Test
    public void shouldCreateCatchupProcessComponentsForEachDatabaseOnStart() throws Throwable
    {
        // given
        List<String> startedCatchupProcs = new ArrayList<>();
        CatchupProcessManager.CatchupProcessFactory factory = db ->
        {
            startedCatchupProcs.add( db.databaseName() );
            CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
            when( catchupProcess.upToDateFuture() ).thenReturn( CompletableFuture.completedFuture( true ) );
            return catchupProcess;
        };
        catchupProcessManager = new CatchupProcessManager( new FakeExecutor(), catchupComponents, databaseService, startStopOnStoreCopy,
                () -> databaseHealth, topologyService, catchUpClient, strategyPipeline, timerService, new CommandIndexTracker(),
                factory, NullLogProvider.getInstance(), versionContextSupplier, pageCursorTracerSupplier, Config.defaults() );
        // when
        catchupProcessManager.init();
        catchupProcessManager.start();

        // then
        assertEquals( startedCatchupProcs, databaseNames );
    }

    @Test
    public void shouldNotRenewTheTimeoutOnPanic()
    {
        // given
        Map<String,CatchupPollingProcess> catchupProcesses = databaseNames.stream()
                .collect( Collectors.toMap( Function.identity(), ignored -> mock( CatchupPollingProcess.class ) ) );

        catchupProcessManager.initDatabaseHealth();
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
