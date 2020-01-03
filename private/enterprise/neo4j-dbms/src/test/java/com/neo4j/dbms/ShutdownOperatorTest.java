/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.StubMultiDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class ShutdownOperatorTest
{
    private DatabaseManager<?> databaseManager = new StubMultiDatabaseManager();
    private ShutdownOperator operator = new ShutdownOperator( databaseManager );
    private DbmsReconciler dbmsReconciler = mock( DbmsReconciler.class );
    private TestOperatorConnector connector = new TestOperatorConnector( dbmsReconciler );
    private List<NamedDatabaseId> databases = asList( NAMED_SYSTEM_DATABASE_ID,
            randomNamedDatabaseId(),
            randomNamedDatabaseId()
    );

    @BeforeEach
    void setup()
    {
        when( dbmsReconciler.reconcile( anyList(), any() ) ).thenReturn( ReconcilerResult.EMPTY );
        operator.connect( connector );
        databases.forEach( databaseManager::createDatabase );
    }

    @Test
    void shouldStopSystemDatabaseLast()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();

        assertEquals( triggerCalls.size(), 2 );
        var initialDesired = triggerCalls.get( 0 ).first();
        var expected = databases.stream()
                .filter( id -> !NAMED_SYSTEM_DATABASE_ID.equals( id ) )
                .collect( Collectors.toMap( NamedDatabaseId::name, id -> new EnterpriseDatabaseState( id, STOPPED ) ) );
        assertEquals( expected, initialDesired );

        var subsequentDesired = triggerCalls.get( 1 ).first();
        expected.put( NAMED_SYSTEM_DATABASE_ID.name(), new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, STOPPED ) );
        assertEquals( expected, subsequentDesired );
    }

    @Test
    void shouldStopAllDatabases()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();
        var finalTrigger = triggerCalls.get( triggerCalls.size() - 1 );
        var expected = databases.stream().collect( Collectors.toMap( NamedDatabaseId::name, id -> new EnterpriseDatabaseState( id, STOPPED ) ) );
        assertEquals( expected, finalTrigger.first() );
    }
}
