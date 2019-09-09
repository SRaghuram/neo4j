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

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class ReadReplicaRoleProcedureTest
{
    private final Context procedureContext = mock( Context.class );
    private final ResourceTracker resourceTracker = mock( ResourceTracker.class );
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
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).register();

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

        databaseManager.givenDatabaseWithConfig()
                .withDatabaseId( databaseId )
                .withStoppedDatabase()
                .register();

        var error = assertThrows( ProcedureException.class,
                () -> procedure.apply( procedureContext, new AnyValue[]{stringValue( databaseId.name() )}, resourceTracker ) );

        assertEquals( DatabaseUnavailable, error.status() );
    }

}
