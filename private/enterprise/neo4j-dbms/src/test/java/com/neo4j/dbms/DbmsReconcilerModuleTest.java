/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.StubMultiDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

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
    void shouldStopAllDatabases() throws Exception
    {
        // given
        var fooId = idRepository.getRaw( "foo" );
        var barId = idRepository.getRaw( "bar" );
        var bazId = idRepository.getRaw( "baz" );

        var desiredDbStates = Stream.of( fooId, barId, bazId )
                                    .collect( Collectors.toMap( NamedDatabaseId::name, id -> new EnterpriseDatabaseState( id, STARTED ) ) );

        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );
        reconcilerModule.start();

        Function<NamedDatabaseId,Stream<Database>> getDb = ( NamedDatabaseId id ) ->
                databaseManager.getDatabaseContext( id )
                               .map( DatabaseContext::database )
                               .stream();

        var databases = Stream.of( fooId, barId, bazId, NAMED_SYSTEM_DATABASE_ID )
                              .flatMap( getDb )
                              .collect( Collectors.toList() );

        // when
        reconcilerModule.stop();

        // then
        assertEquals( 4, databases.size(), "4 databases should have been created" );
        for ( var database : databases )
        {
            verify( database ).stop();
        }
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
        var fooFailure = reconcilerModule.databaseStateService().causeOfFailure( fooId ).orElseThrow();
        assertThat( fooFailure ).hasCause( ex );
        assertSame( reconcilerModule.databaseStateService().stateOfDatabase( fooId ).operatorState(), DIRTY );

        // when
        var startOperator = new LocalDbmsOperator( idRepository );
        startOperator.startDatabase( fooId.name() );
        var startException = assertThrows( Exception.class,
                () -> reconcilerModule.reconciler.reconcile( List.of( startOperator ), ReconcilerRequest.priorityTarget( fooId ).build() ).joinAll(),
                "dirty to not dropped not allowed" );
        // then DIRTY state DB should not able to start
        var message = startException.getCause().getMessage();
        var match = message.contains( "unsupported state transition" );
        assertTrue( Exceptions.contains( startException, "unsupported state transition", IllegalArgumentException.class ) );

        // when
        dropDatabase( fooId, reconcilerModule );

        // then DIRTY DB should be possible to drop always - even if drop itself fails with DatabaseNotFoundException
        assertSame( reconcilerModule.databaseStateService().stateOfDatabase( fooId ).operatorState(), DIRTY );
        var fooFailureCause = reconcilerModule.databaseStateService().causeOfFailure( fooId ).orElseThrow();
        assertTrue( fooFailureCause instanceof DatabaseNotFoundException );

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
        assertThat( reconcilerModule.databaseStateService().causeOfFailure( barId ) ).contains( ex );
        assertSame( reconcilerModule.databaseStateService().stateOfDatabase( barId ).operatorState(), DIRTY );
        var bar = databaseManager.getDatabaseContext( barId ).map( DatabaseContext::database ).orElseThrow();
        verify( bar, times( 1 ) ).stop();

        // when
        dropDatabase( barId, reconcilerModule );

        // then DIRTY DB should be possible to drop always, this time without error
        assertSame( reconcilerModule.databaseStateService().stateOfDatabase( barId ).operatorState(), DROPPED );
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
        var bar = databaseManager.getDatabaseContext( barId ).map( DatabaseContext::database ).orElseThrow();
        verify( bar, times( 1 ) ).stop();

        // when
        reconcilerModule.stop();

        // then DIRTY DB from create failure should be remain - DBManager does not know about it
        assertSame( reconcilerModule.databaseStateService().stateOfDatabase( fooId ).operatorState(), DIRTY );
        var fooFailure = reconcilerModule.databaseStateService().causeOfFailure( fooId ).orElseThrow();
        assertThat( fooFailure ).hasCause( ex );
        assertTrue( databaseManager.getDatabaseContext( fooId ).isEmpty() );

        // then DIRTY DB from prepareDrop failure should be STOPPED - but with no op (stop is not called again)
        assertSame( reconcilerModule.databaseStateService().stateOfDatabase( barId ).operatorState(), STOPPED );
        assertTrue( reconcilerModule.databaseStateService().causeOfFailure( barId ).isEmpty() );
        bar = databaseManager.getDatabaseContext( barId ).map( DatabaseContext::database ).orElseThrow();
        verify( bar, times( 1 ) ).stop();
    }

    private void dropDatabase( NamedDatabaseId fooId, StandaloneDbmsReconcilerModule reconcilerModule )
    {
        var dropOperator = new LocalDbmsOperator( idRepository );
        dropOperator.dropDatabase( fooId.name() );
        reconcilerModule.reconciler.reconcile( List.of( dropOperator ), ReconcilerRequest.priorityTarget( fooId ).build() ).awaitAll();
    }
}
