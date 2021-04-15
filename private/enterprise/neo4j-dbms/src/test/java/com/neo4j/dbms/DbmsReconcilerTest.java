/*
 * Copyright (c) "Neo4j"
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static com.neo4j.dbms.StandaloneDbmsReconcilerModule.createTransitionsTable;
import static java.util.Collections.singletonList;
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
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@ExtendWith( LifeExtension.class )
class DbmsReconcilerTest
{

    @Inject
    private LifeSupport lifeSupport;
    private StubMultiDatabaseManager databaseManager;
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler();

    @BeforeEach
    void setup()
    {
        lifeSupport.add( jobScheduler );
        databaseManager = lifeSupport.add( new StubMultiDatabaseManager( jobScheduler ) );
    }

    @Test
    void shouldNotThrowIfIllegalTransitionRequested()
    {
        // given
        var dbName = "foo";
        var id = idRepository.getByName( "foo" )
                .orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: foo" ) );
        var operator = new ImmutableDbmsOperator( Map.of( dbName, new EnterpriseDatabaseState( id, UNKNOWN )) );
        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );
        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );

        // when INITIAL -> UNKNOWN
        // note: ??? -> UNKNOWN should always be an invalid transition as the UNKNOWN state may not be desired by an operator
        var result = reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() );
        result.await( id );

        // then reconcile should not throw, but db should be failed
        var state = reconciler.getReconcilerEntryOrDefault( id, () -> EnterpriseDatabaseState.unknown( id ) );
        assertEquals( INITIAL, state.operatorState() );
        assertTrue( state.hasFailed() );
    }

    @Test
    void emptyReconciliationRequestsShouldCompleteImmediately() throws InterruptedException
    {
        // given
        var operator = new LocalDbmsOperator();
        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );
        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
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
    void shouldCacheSimpleReconciliationRequests()
    {
        // given
        // an operator desiring foo as started
        var foo = idRepository.getRaw( "foo" );
        var operator = new LocalDbmsOperator();
        // a database manager which blocks on starting databases
        var startingLatch = new CountDownLatch( 1 );
        var isStarting = new AtomicBoolean( false );
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        doAnswer( ignored ->
                  {
                      isStarting.set( true );
                      startingLatch.await();
                      return null;
                  } ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        // a reconciler with a proper multi threaded executor
        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );

        // when
        // the reconciler is already executing a long running job
        operator.startDatabase( foo );
        var startFoo = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEventually( "Reconciler should be starting foo!", isStarting::get, TRUE, 10, SECONDS );

        // and a second job gets created. It waits and is put in an internal cache
        operator.stopDatabase( foo );
        var stopFooA = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );

        // then
        // Further reconciliation attempts should simply return the cached job

        var stopFooB = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEquals( stopFooA, stopFooB, "The reconciler results should be equal for the cached job!" );

        // the reconciler should pick up the latest state at the time each job starts
        operator.startDatabase( foo );
        startingLatch.countDown();
        startFoo.awaitAll();
        stopFooA.awaitAll();
        stopFooB.awaitAll();

        verify( databaseManager, atLeastOnce() ).startDatabase( foo );
        verify( databaseManager, never() ).stopDatabase( foo );
    }

    @Test
    void shouldNotReturnCachedSimpleJobForPriorityRequests()
    {
        // given
        // an operator desiring foo as started
        var foo = idRepository.getRaw( "foo" );
        var operator = new LocalDbmsOperator();

        // a database manager which blocks on starting databases
        var startingLatch = new CountDownLatch( 1 );
        var isStarting = new AtomicBoolean( false );
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        doAnswer( ignored ->
                  {
                      isStarting.set( true );
                      startingLatch.await();
                      return null;
                  } ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        // a reconciler with a proper multi threaded executor
        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
        // when
        // the reconciler is already executing a long running job
        operator.startDatabase( foo );
        var startFoo = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEventually( "Reconciler should be starting foo!", isStarting::get, TRUE, 10, SECONDS );

        // and a second job gets created. It waits and is put in an internal cache
        operator.stopDatabase( foo );
        var stopFooA = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );

        // then
        // A third reconciliation attempts would return the cached job, but its forced, so it won't
        operator.stopDatabase( foo );
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
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        var foo = idRepository.getRaw( "foo" );
        doThrow( failure ).when( databaseManager ).startDatabase( any( NamedDatabaseId.class ) );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
        var databaseStateService = new EnterpriseDatabaseStateService( reconciler, databaseManager );

        // when
        var operator = new LocalDbmsOperator();
        operator.startDatabase( foo );

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
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

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

        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
        var databaseStateService = new EnterpriseDatabaseStateService( reconciler, databaseManager );

        // when
        var operator = new LocalDbmsOperator();
        operator.startDatabase( foo );

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

        var reconciler = new DbmsReconciler( Config.defaults(), NullLogProvider.getInstance(), jobScheduler, transitionsTable );
        var databaseStateService = new EnterpriseDatabaseStateService( reconciler, databaseManager );

        var operator = new LocalDbmsOperator();
        operator.startDatabase( foo );

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
    void shouldLogTransitionFailOnlyOnce()
    {
        // given
        var logProvider = new AssertableLogProvider();
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        var foo = idRepository.getRaw( "foo" );
        var failure = new DatabaseManagementException( "Cannot start" );
        doThrow( failure ).when( databaseManager ).startDatabase( foo );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        var reconciler = new DbmsReconciler( Config.defaults(), logProvider, jobScheduler, transitionsTable );

        var operator = new LocalDbmsOperator();

        // when
        operator.startDatabase( foo );
        reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() ).awaitAll();

        // then
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.ERROR )
                     .containsMessageWithException( "Encountered error when attempting to reconcile database foo " +
                                                    "to state 'online', database remains in state 'offline'", failure );

        // when
        logProvider.clear();
        reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() ).awaitAll();

        // then
        // no error is logged for the failed database
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.ERROR )
                     .doesNotHaveAnyLogs();
        // but there is a warning that it has failed
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.WARN )
                     .containsMessageWithArguments( "Reconciler triggered but the following databases are currently failed and may be ignored: %s. " +
                                                    "Run `SHOW DATABASES` for further information.", "[foo]" );
    }

    @Test
    void shouldLogPanicOnlyOnce()
    {
        // given
        var logProvider = new AssertableLogProvider();
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        var foo = idRepository.getRaw( "foo" );
        Supplier<EnterpriseDatabaseState> initial = () -> EnterpriseDatabaseState.initial( foo );
        var failure = new Exception( "Cause for panic" );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );

        var reconciler = new DbmsReconciler( Config.defaults(), logProvider, jobScheduler, transitionsTable );

        var operator = new StandaloneInternalDbmsOperator( nullLogProvider() );
        var connector = new OperatorConnector( reconciler );
        connector.setOperators( Set.of( operator ) );

        // when
        operator.stopOnPanic( foo, failure );

        // wait for reconciliation be over
        assertEventually( "Foo is stopped", () ->
                reconciler.getReconcilerEntryOrDefault( foo, initial ).operatorState() == STOPPED, TRUE, 10, SECONDS );

        // then
        // no error is logged for the panicked database
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.ERROR )
                     .doesNotHaveAnyLogs();
        // but there is a warning that it has failed
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.WARN )
                     .containsMessagesOnce( "Panicked database foo was reconciled to state 'offline'" );

        // when
        logProvider.clear();
        reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() ).awaitAll();

        // then
        // no error is logged for the panicked database
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.ERROR )
                     .doesNotHaveAnyLogs();
        // but there is a warning that it has failed
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.WARN )
                     .containsMessageWithArguments( "Reconciler triggered but the following databases are currently failed and may be ignored: %s. " +
                                                    "Run `SHOW DATABASES` for further information.", "[foo]" );
        LogAssertions.assertThat( logProvider ).forClass( DbmsReconciler.class ).forLevel( AssertableLogProvider.Level.WARN )
                     .doesNotContainMessage( "Panicked database foo was reconciled to state 'offline'" );
    }

    @Test
    void shouldFailAndErrorForDatabaseWithoutDesiredState() throws Exception
    {
        // given
        var logProvider = new AssertableLogProvider();
        var foo = idRepository.getRaw( "foo" );
        var latchA = new CountDownLatch( 1 );
        var latchB = new CountDownLatch( 1 );

        var databaseManager = new StubMultiDatabaseManager();
        databaseManager.start();
        databaseManager.addOnCreationAction( foo, db ->
        {
            try
            {
                latchB.countDown();
                latchA.await();
            }
            catch ( InterruptedException e )
            {
               Thread.currentThread().interrupt(); // Do nothing
            }
        } );

        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );
        var reconciler = new DbmsReconciler( Config.defaults(), logProvider, jobScheduler, transitionsTable );

        var operator = new MutableDbmsOperator( Map.of( "foo", new EnterpriseDatabaseState( foo, STARTED ) ) );

        // when
        reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() );
        latchB.await();
        var result = reconciler.reconcile( List.of( operator ), ReconcilerRequest.simple() );
        operator.setDesired( Map.of() );
        latchA.countDown();

        // then
        result.await( foo );
        var state = reconciler.getReconcilerEntryOrDefault( foo, () -> EnterpriseDatabaseState.initial( foo ) );
        assertEquals( STARTED, state.operatorState() );
        assertTrue( state.hasFailed() );
        LogAssertions.assertThat( logProvider )
                     .forClass( DbmsReconciler.class )
                     .forLevel( AssertableLogProvider.Level.ERROR )
                     .containsMessages( "No operator desires a state for database foo any more. This is likely an error!" );
    }
}
