/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.procedures.StandaloneDatabaseStateProcedure;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.values.AnyValue;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class CommunityDatabaseStateProcedureTest
{
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final DatabaseStateService stateService = mock( DatabaseStateService.class );
    private final StandaloneDatabaseStateProcedure procedure =
            new StandaloneDatabaseStateProcedure( stateService, idRepository, Config.defaults().get( BoltConnector.advertised_address ).toString() );

    @Test
    void shouldThrowWithInvalidInput()
    {
        assertThrows( IllegalArgumentException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{}, mock( ResourceTracker.class ) ) );

        assertThrows( IllegalArgumentException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{null}, mock( ResourceTracker.class ) ) );

        assertThrows( IllegalArgumentException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{intValue( 42 ),stringValue( "The answer" )}, mock( ResourceTracker.class ) ) );
    }

    @Test
    void shouldThrowWhenDatabaseNotFound() throws ProcedureException
    {
        // given
        when( stateService.stateOfDatabase( any( NamedDatabaseId.class ) ) ).thenReturn( STARTED );
        when( stateService.causeOfFailure( any( NamedDatabaseId.class ) ) ).thenReturn( Optional.empty() );
        var existing = idRepository.getRaw( "existing" );
        var nonExisting = idRepository.getRaw( "nonExisting" );
        idRepository.filter( nonExisting.name() );

        // when/then

        // Should not throw
        procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( existing.name() )}, mock( ResourceTracker.class ) );
        // Should throw
        assertThrows( ProcedureException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( nonExisting.name() )}, mock( ResourceTracker.class ) ) );
    }

    @Test
    void shouldReturnEmptyErrorForNoError() throws ProcedureException
    {
        // given
        when( stateService.stateOfDatabase( any( NamedDatabaseId.class ) ) ).thenReturn( STARTED );
        when( stateService.causeOfFailure( any( NamedDatabaseId.class ) ) ).thenReturn( Optional.empty() );
        var existing = idRepository.getRaw( "existing" );

        // when
        var result = procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( existing.name() )}, mock( ResourceTracker.class ) );
        var returned = Arrays.asList( result.next() );

        // then
        assertEquals( 4, returned.size(), "Procedure result should have 4 columns: role, address, state and error message" );

        var roleColumn = stringValue( "standalone" );
        var addressColumn = stringValue( "localhost:7687" );
        var statusColumn = stringValue( "online" );
        var errorColumn = stringValue( "" );
        assertEquals( Arrays.asList( roleColumn, addressColumn, statusColumn, errorColumn ), returned, "Error column should be empty" );
    }
}
