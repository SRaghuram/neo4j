/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.StubMultiDatabaseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.STOPPED;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

class ShutdownOperatorTest
{
    private TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private DatabaseManager<?> databaseManager = new StubMultiDatabaseManager();
    private ShutdownOperator operator = new ShutdownOperator( databaseManager );
    private DbmsReconciler dbmsReconciler = mock( DbmsReconciler.class );
    private TestOperatorConnector connector = new TestOperatorConnector( dbmsReconciler );
    private List<DatabaseId> databases = asList( SYSTEM_DATABASE_ID,
            databaseIdRepository.defaultDatabase(),
            databaseIdRepository.get( "foo" )
    );

    @BeforeEach
    void setup()
    {
        when( dbmsReconciler.reconcile( anyList(), any() ) ).thenReturn( Reconciliation.EMPTY );
        operator.connect( connector );
        databases.forEach( databaseManager::createDatabase );
    }

    @Test
    void shouldStopSystemDatabaseLast()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();

        Assertions.assertEquals( triggerCalls.size(), 2 );
        var initialDesired = triggerCalls.get( 0 ).first();
        var expected = databases.stream()
                .filter( id -> !SYSTEM_DATABASE_ID.equals( id ) )
                .collect( Collectors.toMap( Function.identity(), ignored -> STOPPED ) );
        assertEquals( expected, initialDesired );

        var subsequentDesired = triggerCalls.get( 1 ).first();
        expected.put( SYSTEM_DATABASE_ID, STOPPED );
        assertEquals( expected, subsequentDesired );
    }

    @Test
    void shouldStopAllDatabases()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();
        var finalTrigger = triggerCalls.get( triggerCalls.size() - 1 );
        Assertions.assertTrue( finalTrigger.first().keySet().containsAll( databases ) );
        var expected = databases.stream().collect( Collectors.toMap( Function.identity(), ignored -> STOPPED ) );
        Assertions.assertEquals( expected, finalTrigger.first() );
    }
}
