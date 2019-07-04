/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.scheduler.CallingThreadJobScheduler;

import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class DbmsReconcilerTest
{
    private MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

    //TODO: refactor after rebase on database id
//    @Test
//    void shouldNotStartInitiallyStarted() throws Exception
//    {
//        // given
//        DatabaseId databaseA = getDatabaseId( "A", 0 );
//        DatabaseId databaseB = getDatabaseId( "B", 0 );
//
//
//        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
//        initialStates.put( databaseA, STARTED );
//        initialStates.put( databaseB, STARTED );
//
//        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, Config.defaults(), NullLogProvider.getInstance(), new CallingThreadJobScheduler(),
//                new PlaceholderDatabaseIdRepository( Config.defaults() ) );
//
//        LocalDbmsOperator operatorA = new LocalDbmsOperator();
//        operatorA.stopDatabase( "A" );
//        TestDbmsOperator operatorB = new TestDbmsOperator( Map.of( databaseB, STARTED ) );
//
//        List<DbmsOperator> operators = List.of( operatorA, operatorB );
//
//        // when
//        reconciler.reconcile( operators, false );
//
//        // then
//        verifyNoMoreInteractions( databaseManager );
//    }
//
//    @Test
//    void shouldStopInitiallyStarted() throws Exception
//    {
//        // given
//        DatabaseId databaseA = getDatabaseId( "A", 0 );
//        DatabaseId databaseB = getDatabaseId( "B", 0 );
//
//        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
//        initialStates.put( databaseA, STARTED );
//        initialStates.put( databaseB, STARTED );
//
//        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, initialStates,
//                Config.defaults(), NullLogProvider.getInstance(), new CallingThreadJobScheduler() );
//
//        TestDbmsOperator operatorA = new TestDbmsOperator( Map.of( databaseA, STOPPED ) );
//        TestDbmsOperator operatorB = new TestDbmsOperator( Map.of( databaseB, STOPPED ) );
//
//        List<DbmsOperator> operators = List.of( operatorA, operatorB );
//
//        // when
//        reconciler.reconcile( operators, false );
//
//        // then
//        verify( databaseManager ).stopDatabase( databaseA );
//        verify( databaseManager ).stopDatabase( databaseB );
//    }
//
//    @Test
//    void shouldNotTouchOperatorUnknownDatabases() throws Exception
//    {
//        // given
//        DatabaseId databaseA = getDatabaseId( "A", 0 );
//        DatabaseId databaseB = getDatabaseId( "B", 0 );
//
//        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
//        initialStates.put( databaseA, STARTED );
//        initialStates.put( databaseB, STOPPED );
//
//        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, initialStates,
//               Config.defaults(), NullLogProvider.getInstance(), new CallingThreadJobScheduler() );
//
//        TestDbmsOperator operatorA = new TestDbmsOperator( emptyMap() );
//        TestDbmsOperator operatorB = new TestDbmsOperator( emptyMap() );
//
//        List<DbmsOperator> operators = List.of( operatorA, operatorB );
//
//        // when
//        reconciler.reconcile( operators, false );
//
//        // then
//        verifyNoMoreInteractions( databaseManager );
//    }
//
//    //TODO: remove after rebase
//    private DatabaseId getDatabaseId( String databaseName, int seed )
//    {
//        return new DatabaseId( databaseName, UUID.nameUUIDFromBytes( seed( seed ) ) );
//    }
//
//    private byte[] seed( int seed )
//    {
//        var bytes = new byte[10];
//        Random random = new Random( seed );
//        random.nextBytes( bytes );
//        return bytes;
//    }
}
