/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalDbmsOperatorTest
{
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DbmsReconciler reconciler = mock( DbmsReconciler.class );
    private final OperatorConnector connector = new OperatorConnector( reconciler );
    private final LocalDbmsOperator operator = new LocalDbmsOperator();

    private final String databaseName = "my.db";
    private final NamedDatabaseId databaseId = DatabaseIdFactory.from( databaseName, UUID.randomUUID() );
    private final ReconcilerRequest reconcilerRequest = ReconcilerRequest.priorityTarget( databaseId ).build();

    @BeforeEach
    void setup()
    {
        when( reconciler.reconcile( anyCollection(), any( ReconcilerRequest.class ) ) ).thenReturn( ReconcilerResult.EMPTY );
        connector.setOperators( List.of( operator ) );
    }

    @Test
    void shouldBeAbleToDropDatabase()
    {
        operator.dropDatabase( databaseId );
        verify( reconciler, times( 1 ) ).reconcile( Set.of( operator ), reconcilerRequest );

        assertEquals( DROPPED, operatorState( databaseIdRepository.getByName( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStartDatabase()
    {
        operator.startDatabase( databaseId );
        verify( reconciler, times( 1 ) ).reconcile( Set.of( operator ), reconcilerRequest );

        assertEquals( STARTED, operatorState( databaseIdRepository.getByName( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStopDatabase()
    {
        operator.stopDatabase( databaseId );
        verify( reconciler, times( 1 ) ).reconcile( Set.of( operator ), reconcilerRequest );

        assertEquals( STOPPED, operatorState( databaseIdRepository.getByName( databaseName ) ) );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private EnterpriseOperatorState operatorState( Optional<NamedDatabaseId> databaseId )
    {
        return databaseId.flatMap( id -> Optional.ofNullable( operator.desired().get( id.name() ) ) )
                .map( EnterpriseDatabaseState::operatorState )
                .orElse( UNKNOWN );
    }
}
