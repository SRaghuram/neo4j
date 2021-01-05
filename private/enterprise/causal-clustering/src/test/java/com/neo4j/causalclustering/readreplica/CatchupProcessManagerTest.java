/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.schedule.CountingTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.error_handling.DatabasePanicReason;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.readreplica.CatchupProcessFactory.CatchupProcessComponents;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.readreplica.CatchupProcessManager.Timers.TX_PULLER_TIMER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
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
    private LifeSupport life;

    private final CatchupComponentsRepository catchupComponents = mock( CatchupComponentsRepository.class );
    private final Panicker panicker = mock( Panicker.class );

    private final StubClusteredDatabaseManager databaseService = new StubClusteredDatabaseManager();
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId = databaseIdRepository.getRaw( "db1" );
    private final ReadReplicaDatabaseContext databaseContext = mock( ReadReplicaDatabaseContext.class );
    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final CountingTimerService timerService = new CountingTimerService( scheduler, nullLogProvider() );

    @BeforeEach
    void before()
    {
        //Mock the components of CatchupComponentsRepository
        databaseService.registerDatabase( namedDatabaseId, getMockDatabase( namedDatabaseId ) );
        CatchupComponents components = new CatchupComponents( mock( RemoteStore.class ), mock( StoreCopyProcess.class ) );

        //Wire these mocked components to the ServerModule mock
        when( catchupComponents.componentsFor( any() ) ).thenReturn( Optional.of( components ) );
        when( databaseContext.kernelDatabase() ).thenReturn( mock( Database.class ) );
        when( databaseContext.monitors() ).thenReturn( mock( Monitors.class ) );
    }

    private CatchupProcessManager createProcessManager( CatchupPollingProcess catchupProcess )
    {
        var catchupProcessFactory = mock( CatchupProcessFactory.class );
        var catchupProcessComponents = new CatchupProcessComponents( catchupProcess, mock( BatchingTxApplier.class ) );
        when( catchupProcessFactory.create( any() ) ).thenReturn( catchupProcessComponents );
        var catchupProcessManager = new CatchupProcessManager(
                databaseContext, panicker, timerService, nullLogProvider(), Config.defaults(), catchupProcessFactory );
        life.add( catchupProcessManager );
        return spy( catchupProcessManager );
    }

    private void createProcessManager( CatchupProcessFactory catchupProcessFactory, TimerService timerService )
    {
        var catchupProcessManager = new CatchupProcessManager(
                databaseContext, panicker, timerService, nullLogProvider(), Config.defaults(), catchupProcessFactory );
        life.add( catchupProcessManager );
    }

    private ClusteredDatabaseContext getMockDatabase( NamedDatabaseId namedDatabaseId )
    {
        return databaseService.givenDatabaseWithConfig()
                .withDatabaseId( namedDatabaseId )
                .withDependencies( mock( Dependencies.class ) )
                .register();
    }

    @Test
    void shouldTickCatchupProcessOnTimeout()
    {
        // given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        createProcessManager( catchupProcess );

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
        CatchupProcessManager catchupProcessManager = createProcessManager( catchupProcess );

        catchupProcessManager.panicDatabase( DatabasePanicReason.Test, new RuntimeException( "Don't panic Mr. Mainwaring!" ) );

        Timer timer = spy( single( timerService.getTimers( TX_PULLER_TIMER ) ) );

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( timer, never() ).reset();
    }

    @Test
    void pauseCatchupProcessShouldBeIdempotent()
    {
        //given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        when( catchupProcess.isStoryCopy() ).thenReturn( false );
        CatchupProcessManager catchupProcessManager = createProcessManager( catchupProcess );

        //when
        assertTrue( catchupProcessManager.pauseCatchupProcess() );
        assertFalse( catchupProcessManager.pauseCatchupProcess() );
    }

    @Test
    void resumeCatchupProcessShouldBeIdempotent()
    {
        //given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        when( catchupProcess.isStoryCopy() ).thenReturn( false );
        CatchupProcessManager catchupProcessManager = createProcessManager( catchupProcess );

        //when
        assertTrue( catchupProcessManager.pauseCatchupProcess() );
        assertTrue( catchupProcessManager.resumeCatchupProcess() );
        assertFalse( catchupProcessManager.resumeCatchupProcess() );
    }

    @Test
    void shouldFailToStopCatchupPollingIfCatchupPollingHasStoryCopyState()
    {
        //given
        CatchupPollingProcess catchupProcess = mock( CatchupPollingProcess.class );
        when( catchupProcess.isStoryCopy() ).thenReturn( true );
        CatchupProcessManager catchupProcessManager = createProcessManager( catchupProcess );

        //when
        assertThrows( IllegalStateException.class, catchupProcessManager::pauseCatchupProcess );
    }

    @Test
    void shouldAbortOnStopWhilstPulling() throws Exception
    {
        var catchupProcess = mock( CatchupPollingProcess.class );
        CompletableFuture<Void> cf = new CompletableFuture<>();
        Semaphore onTick = new Semaphore( 0 );
        doAnswer( ignored ->
        {
            onTick.release();
            return cf;
        } ).when( catchupProcess ).tick();
        doAnswer( ignored -> cf.complete( null ) ).when( catchupProcess ).stop();

        var catchupProcessFactory = mock( CatchupProcessFactory.class );
        when( catchupProcessFactory.create( any() ) ).thenReturn( new CatchupProcessComponents( catchupProcess, mock( BatchingTxApplier.class ) ) );

        var scheduler = new CentralJobScheduler( Clocks.nanoClock() )
        {
        };
        life.add( scheduler );

        var timerService = new TimerService( scheduler, nullLogProvider() );
        createProcessManager( catchupProcessFactory, timerService );

        // when: onTimeout() should block on the CompletableFuture above
        onTick.acquire();

        // then: on shutdown() we should not be blocked, because our custom CatchupProcess completes the CompletableFuture
        life.shutdown();
    }
}
