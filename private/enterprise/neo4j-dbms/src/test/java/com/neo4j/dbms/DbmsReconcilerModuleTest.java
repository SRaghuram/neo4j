/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.dbms.database.StubMultiDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.StandaloneDbmsReconcilerModule.createTransitionsTable;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ExtendWith( LifeExtension.class )
class DbmsReconcilerModuleTest
{
    @Inject
    private LifeSupport lifeSupport;
    private StubMultiDatabaseManager databaseManager;
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final EnterpriseSystemGraphDbmsModel dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler();

    @BeforeEach
    void setup()
    {
        lifeSupport.add( jobScheduler );
        databaseManager = lifeSupport.add( new StubMultiDatabaseManager( jobScheduler ) );
    }

    @Test
    void shouldStartInitialDatabases() throws Exception
    {
        // given
        when( dbmsModel.getDatabaseStates() ).thenReturn(
                singletonMap( idRepository.defaultDatabase().name(), new EnterpriseDatabaseState( idRepository.defaultDatabase(), STARTED ) ) );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );

        // when
        reconcilerModule.start();

        // then
        var system = databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID );
        var neo4j = databaseManager.getDatabaseContext( idRepository.defaultDatabase() );
        assertTrue( system.isPresent(), "System db should have been created" );
        assertTrue( neo4j.isPresent(), "Default db should have been created" );
        verify( system.get().database() ).start();
        verify( neo4j.get().database() ).start();
    }

    @Test
    void shouldThrowOnStartIfSystemDbFails()
    {
        // given
        var mockSystemDb = databaseManager.createDatabase( NAMED_SYSTEM_DATABASE_ID );
        var mockKernelSystemDb = mockSystemDb.database();
        doThrow( new RuntimeException() ).when( mockKernelSystemDb ).start();

        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );

        // when / then
        assertThrows( Exception.class, reconcilerModule::start );
    }

    @Test
    void emptyReconciliationRequestsShouldCompleteImmediately() throws InterruptedException
    {
        // given
        var operator = new LocalDbmsOperator( idRepository );
        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );
        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
        var waitingFinished = new CountDownLatch( 1 );
        var result = reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() );

        // when
        CompletableFuture.runAsync( () ->
        {
            result.awaitAll();
            waitingFinished.countDown();
        } );

        // then
        waitingFinished.await( 10, SECONDS );
    }

    @Test
    void shouldStopAllDatabases() throws Exception
    {
        // given
        var fooId = idRepository.getRaw( "foo" );
        var barId = idRepository.getRaw( "bar" );
        var bazId = idRepository.getRaw( "baz" );

        Map<String,EnterpriseDatabaseState> desiredDbStates = Stream.of( fooId, barId, bazId )
                .collect( Collectors.toMap( NamedDatabaseId::name, id -> new EnterpriseDatabaseState( id, STARTED ) ) );

        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );
        reconcilerModule.start();

        Function<NamedDatabaseId,Stream<Database>> getDb = ( NamedDatabaseId id ) -> databaseManager.getDatabaseContext( id )
                .map( DatabaseContext::database )
                .stream();

        var databases = Stream.of( fooId, barId, bazId, NAMED_SYSTEM_DATABASE_ID )
                .flatMap( getDb )
                .collect( Collectors.toList() );

        // when
        reconcilerModule.stop();

        // then
        assertEquals( 4, databases.size(), "4 databases should have been created" );
        for ( Database database : databases )
        {
            verify( database ).stop();
        }
    }

    @Test
    void shouldCacheSimpleReconciliationRequests() throws Exception
    {
        // given
        // an operator desiring foo as started
        var foo = idRepository.getRaw( "foo" );
        var operator = new LocalDbmsOperator( idRepository );
        // a database manager which blocks on starting databases
        CountDownLatch startingLatch = new CountDownLatch( 1 );
        AtomicBoolean isStarting = new AtomicBoolean( false );
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        doAnswer( ignored ->
        {
            isStarting.set( true );
            startingLatch.await();
            return null;
        } ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        // a reconciler with a proper multi threaded executor
        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );

        // when
        // the reconciler is already executing a long running job
        operator.startDatabase( foo.name() );
        var startFoo = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEventually( "Reconciler should be starting foo!", isStarting::get, TRUE, 10, SECONDS );

        // and a second job gets created. It waits and is put in an internal cache
        operator.stopDatabase( foo.name() );
        var stopFooA = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );

        // then
        // Further reconciliation attempts should simply return the cached job

        var stopFooB = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEquals( stopFooA, stopFooB, "The reconciler results should be equal for the cached job!" );

        // the reconciler should pick up the latest state at the time each job starts
        operator.startDatabase( foo.name() );
        startingLatch.countDown();
        startFoo.awaitAll();
        stopFooA.awaitAll();
        stopFooB.awaitAll();

        verify( databaseManager, atLeastOnce() ).startDatabase( foo );
        verify( databaseManager, never() ).stopDatabase( foo );
    }

    @Test
    void shouldNotReturnCachedSimpleJobForPriorityRequests() throws Exception
    {
        // given
        // an operator desiring foo as started
        var foo = idRepository.getRaw( "foo" );
        var operator = new LocalDbmsOperator( idRepository );

        // a database manager which blocks on starting databases
        CountDownLatch startingLatch = new CountDownLatch( 1 );
        AtomicBoolean isStarting = new AtomicBoolean( false );
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        doAnswer( ignored ->
        {
            isStarting.set( true );
            startingLatch.await();
            return null;
        } ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        // a reconciler with a proper multi threaded executor
        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
        // when
        // the reconciler is already executing a long running job
        operator.startDatabase( foo.name() );
        var startFoo = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEventually( "Reconciler should be starting foo!", isStarting::get, TRUE, 10, SECONDS );

        // and a second job gets created. It waits and is put in an internal cache
        operator.stopDatabase( foo.name() );
        var stopFooA = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );

        // then
        // A third reconciliation attempts would return the cached job, but its forced, so it won't
        operator.stopDatabase( foo.name() );
        var stopFooB = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.priorityTarget( foo ).build() );

        // then
        assertNotEquals( stopFooA, stopFooB, "The reconciler results should not be equal as forced jobs should not be cached!" );

        startingLatch.countDown();
        startFoo.awaitAll();
        stopFooA.awaitAll();
        stopFooB.awaitAll();
    }

    static Stream<Throwable> failures()
    {
        return Stream.of( new RuntimeException(), new Error() );
    }

    @ParameterizedTest
    @MethodSource( value = "failures" )
    void shouldCatchAsFailure( Throwable failure )
    {
        // given
        MultiDatabaseManager databaseManager = mock( MultiDatabaseManager.class );

        var foo = idRepository.getRaw( "foo" );
        doThrow( failure ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), nullLogProvider(), jobScheduler, transitionsTable );
        var databaseStateService = new LocalDatabaseStateService( reconciler );

        // when
        var operator = new LocalDbmsOperator( idRepository );
        operator.startDatabase( "foo" );

        reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() ).awaitAll();
        var startFailure = databaseStateService.causeOfFailure( foo );

        // then
        assertTrue( startFailure.isPresent() );
        assertEquals( failure, startFailure.get() );
    }

    @Test
    void shouldDoCleanupInTransitionFails()
    {
        // given
        var databaseManager = mock( MultiDatabaseManager.class );

        var foo = idRepository.getRaw( "foo" );
        var failure = new RuntimeException();
        doThrow( failure ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionWithCleanup = Transition.from( INITIAL )
                .doTransition( databaseManager::startDatabase )
                .ifSucceeded( STARTED )
                .ifFailedThenDo( databaseManager::stopDatabase, STOPPED );
        var transitionsTable = TransitionsTable.builder()
                .from( INITIAL ).to( STARTED ).doTransitions( transitionWithCleanup )
                .build();

        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), nullLogProvider(), jobScheduler, transitionsTable );
        var databaseStateService = new LocalDatabaseStateService( reconciler );

        // when
        var operator = new LocalDbmsOperator( idRepository );
        operator.startDatabase( "foo" );

        reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() ).awaitAll();
        var startFailure = databaseStateService.causeOfFailure( foo );

        // then
        assertTrue( startFailure.isPresent() );
        assertEquals( failure, startFailure.get() );
        verify( databaseManager ).stopDatabase( foo );
    }

    @Test
    void priorityRequestsShouldIgnoreAndHealFailedStates()
    {
        // given
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        var foo = idRepository.getRaw( "foo" );
        var failure = new RuntimeException( "An error has occurred" );

        doThrow( failure )
                .doNothing() // Second attempt succeeds
                .when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), nullLogProvider(), jobScheduler, transitionsTable );
        var databaseStateService = new LocalDatabaseStateService( reconciler );

        var operator = new LocalDbmsOperator( idRepository );
        operator.startDatabase( "foo" );

        // when/then
        assertThrows( DatabaseManagementException.class, () -> reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() ).join( foo ) );
        var startFailure = databaseStateService.causeOfFailure( foo );
        assertTrue( startFailure.isPresent() );
        assertEquals( failure, startFailure.get() );

        verify( databaseManager ).startDatabase( foo );

        // when
        reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() ).await( foo );

        // then
        verify( databaseManager, atMostOnce() ).startDatabase( foo );

        // when
        reconciler.reconcile( List.of( operator ), ReconcilerRequest.priorityTarget( foo ).build() ).join( foo );

        // then
        startFailure = databaseStateService.causeOfFailure( foo );
        assertTrue( startFailure.isEmpty() );
        verify( databaseManager, times( 2 ) ).startDatabase( foo );
    }

    @Test
    void onlyDropIsAvailableFromInitialDirty() throws Exception
    {
        // given
        var ex = new RuntimeException( "Cause of dirty" );
        var fooId = idRepository.getRaw( "foo" );
        var desiredDbStates = Map.of( fooId.name(), new EnterpriseDatabaseState( fooId, STARTED ) );
        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        databaseManager.addOnCreationAction( fooId, ignored ->
        {
            throw ex;
        } );

        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );
        // when
        reconcilerModule.start();

        // then DB cannot created > its state should be DIRTY with given exception
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( fooId ).get().getCause() == ex );
        assertTrue( reconcilerModule.databaseStateService().stateOfDatabase( fooId ) == DIRTY );

        // when
        var startOperator = new LocalDbmsOperator( idRepository );
        startOperator.startDatabase( fooId.name() );
        var startException = assertThrows( Exception.class,
                () -> reconcilerModule.reconciler.reconcile( List.of( startOperator ), ReconcilerRequest.priorityTarget( fooId ).build() ).awaitAll(),
                "dirty to not dropped not allowed" );
        // then DIRTY state DB should not able to start
        assertTrue( startException.getMessage().contains( "unsupported state transition" ) );

        // when
        dropDatabase( fooId, reconcilerModule );

        // then DIRTY DB should be possible to drop always - even if drop itself fails with DatabaseNotFoundException
        assertTrue( reconcilerModule.databaseStateService().stateOfDatabase( fooId ) == DIRTY );
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( fooId ).get() instanceof DatabaseNotFoundException );

        reconcilerModule.stop();
    }

    @Test
    void onlyDropIsAvailableFromDroppedDirty() throws Exception
    {
        // given
        var ex = new RuntimeException( "Cause of dirty" );
        var barId = idRepository.getRaw( "foo" );
        var desiredDbStates = Map.of( barId.name(), new EnterpriseDatabaseState( barId, STARTED ) );
        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        databaseManager.addOnCreationAction( barId, database ->
        {
            doThrow( ex ).when( database ).prepareToDrop();
        } );

        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );

        // when
        reconcilerModule.start();
        dropDatabase( barId, reconcilerModule );

        // then DB cannot be dropped > since prepareDrop fails it should end up in DIRTY with given exception but stopDatabase must have been called
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( barId ).get() == ex );
        assertTrue( reconcilerModule.databaseStateService().stateOfDatabase( barId ) == DIRTY );
        verify( databaseManager.getDatabaseContext( barId ).get().database(), times( 1 ) ).stop();

        // when
        dropDatabase( barId, reconcilerModule );

        // then DIRTY DB should be possible to drop always, this time without error
        assertTrue( reconcilerModule.databaseStateService().stateOfDatabase( barId ) == DROPPED );
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( barId ).isEmpty() );

        reconcilerModule.stop();
    }

    @Test
    void dirtyDatabaseCanBeStoppedAtShutdown() throws Exception
    {
        // given
        var ex = new RuntimeException( "Cause of dirty" );
        var fooId = idRepository.getRaw( "foo" );
        var barId = idRepository.getRaw( "bar" );
        var desiredDbStates = Map.of( fooId.name(), new EnterpriseDatabaseState( fooId, STARTED ),
                barId.name(), new EnterpriseDatabaseState( barId, STARTED ) );
        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        databaseManager.addOnCreationAction( fooId, ignored ->
        {
            throw ex;
        } );
        databaseManager.addOnCreationAction( barId, database ->
        {
            doThrow( ex ).when( database ).prepareToDrop();
        } );

        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );

        // when
        reconcilerModule.start();
        dropDatabase( barId, reconcilerModule );

        // then stop is already called once
        verify( databaseManager.getDatabaseContext( barId ).get().database(), times( 1 ) ).stop();

        // when
        reconcilerModule.stop();

        // then DIRTY DB from create failure should be remain - DBManager does not know about it
        assertTrue( reconcilerModule.databaseStateService().stateOfDatabase( fooId ) == DIRTY );
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( fooId ).get().getCause() == ex );
        assertTrue( databaseManager.getDatabaseContext( fooId ).isEmpty() );

        // then DIRTY DB from prepareDrop failure should be STOPPED - but with no op (stop is not called again)
        assertTrue( reconcilerModule.databaseStateService().stateOfDatabase( barId ) == STOPPED );
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( barId ).isEmpty() );
        verify( databaseManager.getDatabaseContext( barId ).get().database(), times( 1 ) ).stop();
    }

    private void dropDatabase( NamedDatabaseId fooId, StandaloneDbmsReconcilerModule reconcilerModule )
    {
        var dropOperator = new LocalDbmsOperator( idRepository );
        dropOperator.dropDatabase( fooId.name() );
        reconcilerModule.reconciler.reconcile( List.of( dropOperator ), ReconcilerRequest.priorityTarget( fooId ).build() ).awaitAll();
    }
}
