/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalDbmsOperatorTest
{
    private DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private OperatorConnector connector = mock( OperatorConnector.class );
    private LocalDbmsOperator operator = new LocalDbmsOperator( databaseIdRepository );

    private String databaseName = "my.db";
    private NamedDatabaseId databaseId = DatabaseIdFactory.from( databaseName, UUID.randomUUID() );

    @BeforeEach
    void setup()
    {
        when( connector.trigger( ReconcilerRequest.priorityTarget( databaseId ).build() ) ).thenReturn( ReconcilerResult.EMPTY );
        operator.connect( connector );
    }

    @Test
    void shouldBeAbleToDropDatabase()
    {
        operator.dropDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger( ReconcilerRequest.priorityTarget( databaseId ).build() );

        assertEquals( DROPPED, operatorState( databaseIdRepository.getByName( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStartDatabase()
    {
        operator.startDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger( ReconcilerRequest.priorityTarget( databaseId ).build() );

        assertEquals( STARTED, operatorState( databaseIdRepository.getByName( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStopDatabase()
    {
        operator.stopDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger( ReconcilerRequest.priorityTarget( databaseId ).build() );

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
