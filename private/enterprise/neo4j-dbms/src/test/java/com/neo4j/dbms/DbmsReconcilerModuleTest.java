/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.test.assertion.Assert.assertEventually;

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
    void shouldStartInitialDatabases()
    {
        // given
        when( dbmsModel.getDatabaseStates() ).thenReturn(
                singletonMap( idRepository.defaultDatabase().name(), new EnterpriseDatabaseState( idRepository.defaultDatabase(), STARTED ) ) );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );

        // when
        reconcilerModule.start();

        // then
        var system = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID );
        var neo4j = databaseManager.getDatabaseContext( idRepository.defaultDatabase() );
        assertTrue( system.isPresent(), "System db should have been created" );
        assertTrue( neo4j.isPresent(), "Default db should have been created" );
        verify( system.get().database() ).start();
        verify( neo4j.get().database() ).start();
    }

    @Test
    void shouldStopAllDatabases()
    {
        // given
        var fooId = idRepository.getRaw( "foo" );
        var barId = idRepository.getRaw( "bar" );
        var bazId = idRepository.getRaw( "baz" );

        Map<String,EnterpriseDatabaseState> desiredDbStates = Stream.of( fooId, barId, bazId )
                .collect( Collectors.toMap( DatabaseId::name, id -> new EnterpriseDatabaseState( id, STARTED ) ) );

        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );
        reconcilerModule.start();

        Function<DatabaseId,Stream<Database>> getDb = ( DatabaseId id ) -> databaseManager.getDatabaseContext( id )
                .map( DatabaseContext::database )
                .stream();

        var databases = Stream.of( fooId, barId, bazId, SYSTEM_DATABASE_ID )
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
        CountDownLatch startingLatch  = new CountDownLatch( 1 );
        AtomicBoolean isStarting = new AtomicBoolean( false );
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        doAnswer( ignored ->
        {
            isStarting.set( true );
            startingLatch.await();
            return null;
        } ).when( databaseManager ).startDatabase( any( DatabaseId.class ) );

        // a reconciler with a proper multi threaded executor
        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), NullLogProvider.getInstance(), jobScheduler );

        // when
        // the reconciler is already executing a long running job
        operator.startDatabase( foo.name() );
        var startFoo = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEventually( "Reconciler should be starting foo!", isStarting::get, is( true ), 10, SECONDS );

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
    void shouldNotCacheForceReconciliationRequests() throws Exception
    {
        // given
        // an operator desiring foo as started
        var foo = idRepository.getRaw( "foo" );
        var operator = new LocalDbmsOperator( idRepository );

        // a database manager which blocks on starting databases
        CountDownLatch startingLatch  = new CountDownLatch( 1 );
        AtomicBoolean isStarting = new AtomicBoolean( false );
        MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

        doAnswer( ignored ->
        {
            isStarting.set( true );
            startingLatch.await();
            return null;
        } ).when( databaseManager ).startDatabase( any( DatabaseId.class ) );

        // a reconciler with a proper multi threaded executor
        var reconciler = new DbmsReconciler( databaseManager, Config.defaults(), NullLogProvider.getInstance(), jobScheduler );
        // when
        // the reconciler is already executing a long running job
        operator.startDatabase( foo.name() );
        var startFoo = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );
        assertEventually( "Reconciler should be starting foo!", isStarting::get, is( true ), 10, SECONDS );

        // and a second job gets created. It waits and is put in an internal cache
        operator.stopDatabase( foo.name() );
        var stopFooA = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() );

        // then
        // A third reconciliation attempts would return the cached job, but its forced, so it won't
        operator.stopDatabase( foo.name() );
        var stopFooB = reconciler.reconcile( singletonList( operator ), ReconcilerRequest.force() );

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

        DatabaseId foo = idRepository.getRaw( "foo" );
        doThrow( failure ).when( databaseManager ).startDatabase( any( DatabaseId.class ) );

        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, Config.defaults(), nullLogProvider(), jobScheduler );

        // when
        LocalDbmsOperator operator = new LocalDbmsOperator( idRepository );
        operator.startDatabase( "foo" );

        reconciler.reconcile( singletonList( operator ), ReconcilerRequest.simple() ).awaitAll();
        Optional<Throwable> startFailure = reconciler.causeOfFailure( foo );

        // then
        assertTrue( startFailure.isPresent() );
        assertEquals( failure, startFailure.get() );
    }
}
