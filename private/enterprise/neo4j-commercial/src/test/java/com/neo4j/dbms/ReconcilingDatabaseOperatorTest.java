/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class ReconcilingDatabaseOperatorTest
{
    private DatabaseManager databaseManager = mock( DatabaseManager.class );
    private DatabaseIdRepository databaseIdRepository = new PlaceholderDatabaseIdRepository( Config.defaults() );

    @Test
    void shouldNotStartInitiallyStarted()
    {
        // given
        DatabaseId databaseA = databaseIdRepository.get( "A" );
        DatabaseId databaseB = databaseIdRepository.get( "B" );

        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
        initialStates.put( databaseA, STARTED );
        initialStates.put( databaseB, STARTED );

        ReconcilingDatabaseOperator reconciler = new ReconcilingDatabaseOperator( databaseManager, initialStates );

        TestOperator operatorA = new TestOperator( Map.of( databaseA, STARTED ) );
        TestOperator operatorB = new TestOperator( Map.of( databaseB, STARTED ) );

        List<Operator> operators = List.of( operatorA, operatorB );

        // when
        reconciler.reconcile( operators );

        // then
        verifyNoMoreInteractions( databaseManager );
    }

    @Test
    void shouldStopInitiallyStarted()
    {
        // given
        DatabaseId databaseA = databaseIdRepository.get( "A" );
        DatabaseId databaseB = databaseIdRepository.get( "B" );

        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
        initialStates.put( databaseA, STARTED );
        initialStates.put( databaseB, STARTED );

        ReconcilingDatabaseOperator reconciler = new ReconcilingDatabaseOperator( databaseManager, initialStates );

        TestOperator operatorA = new TestOperator( Map.of( databaseA, STOPPED ) );
        TestOperator operatorB = new TestOperator( Map.of( databaseB, STOPPED ) );

        List<Operator> operators = List.of( operatorA, operatorB );

        // when
        reconciler.reconcile( operators );

        // then
        verify( databaseManager ).stopDatabase( databaseA );
        verify( databaseManager ).stopDatabase( databaseB );
    }

    @Test
    void shouldNotTouchOperatorUnknownDatabases()
    {
        // given
        DatabaseId databaseA = databaseIdRepository.get( "A" );
        DatabaseId databaseB = databaseIdRepository.get( "B" );

        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
        initialStates.put( databaseA, STARTED );
        initialStates.put( databaseB, STOPPED );

        ReconcilingDatabaseOperator reconciler = new ReconcilingDatabaseOperator( databaseManager, initialStates );

        TestOperator operatorA = new TestOperator( emptyMap() );
        TestOperator operatorB = new TestOperator( emptyMap() );

        List<Operator> operators = List.of( operatorA, operatorB );

        // when
        reconciler.reconcile( operators );

        // then
        verifyNoMoreInteractions( databaseManager );
    }
}
