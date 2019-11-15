/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.discovery.RoleInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.AnyValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class ReadReplicaRoleProcedureTest
{
    private final Context procedureContext = mock( Context.class );
    private final ResourceTracker resourceTracker = mock( ResourceTracker.class );
    private final DatabaseAvailabilityGuard availabilityGuard = mock( DatabaseAvailabilityGuard.class );
    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final ReadReplicaRoleProcedure procedure = new ReadReplicaRoleProcedure( databaseManager );

    @Test
    void shouldThrowWhenDatabaseNameNotSpecified()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.apply( procedureContext, new AnyValue[]{}, resourceTracker ) );
    }

    @Test
    void shouldThrowWhenDatabaseNameIsNull()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.apply( procedureContext, new AnyValue[]{null}, resourceTracker ) );
    }

    @Test
    void shouldThrowWhenDatabaseNameIsNotString()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.apply( procedureContext, new AnyValue[]{intValue( 42 )}, resourceTracker ) );
    }

    @Test
    void shouldReturnReadReplica() throws Exception
    {
        var databaseId = databaseManager.databaseIdRepository().getRaw( "foo" );
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withDatabaseAvailabilityGuard( availabilityGuard ).register();

        var result = procedure.apply( procedureContext, new AnyValue[]{stringValue( databaseId.name() )}, resourceTracker );

        assertTrue( result.hasNext() );

        var row = result.next();
        assertFalse( result.hasNext() );
        assertEquals( 1, row.length );

        var element = row[0];
        assertEquals( stringValue( RoleInfo.READ_REPLICA.toString() ), element );
    }

    @Test
    void shouldHaveCorrectName()
    {
        assertEquals( new QualifiedName( List.of( "dbms", "cluster" ), "role" ), procedure.signature().name() );
    }

    @Test
    void shouldBeASystemProcedure()
    {
        assertTrue( procedure.signature().systemProcedure() );
    }

    @Test
    void shouldThrowForStoppedDatabase()
    {
        var databaseId = databaseManager.databaseIdRepository().getRaw( "bar" );
        when( availabilityGuard.isAvailable() ).thenReturn( false );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withDatabaseAvailabilityGuard( availabilityGuard ).register();

        var error = assertThrows( ProcedureException.class,
                () -> procedure.apply( procedureContext, new AnyValue[]{stringValue( databaseId.name() )}, resourceTracker ) );

        assertEquals( DatabaseUnavailable, error.status() );
    }

    @Test
    void shouldThrowForNonExistingDatabase()
    {
        var nonExistingDatabaseName = "baz";
        var databaseManager = mock( DatabaseManager.class );
        var databaseIdRepository = mock( DatabaseIdRepository.Caching.class );
        when( databaseIdRepository.getByName( nonExistingDatabaseName ) ).thenReturn( Optional.empty() );
        when( databaseManager.databaseIdRepository() ).thenReturn( databaseIdRepository );

        var procedure = new ReadReplicaRoleProcedure( databaseManager );

        var error = assertThrows( ProcedureException.class,
                () -> procedure.apply( procedureContext, new AnyValue[]{stringValue( nonExistingDatabaseName )}, resourceTracker ) );

        assertEquals( DatabaseNotFound, error.status() );
    }
}
