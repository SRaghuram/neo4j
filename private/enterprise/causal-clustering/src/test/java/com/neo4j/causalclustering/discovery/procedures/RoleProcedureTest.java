/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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

public abstract class RoleProcedureTest

{
    protected final DatabaseManager<DatabaseContext> databaseManager = mock( DatabaseManager.class );
    protected final DatabaseContext databaseContext = mock( DatabaseContext.class );
    private final String databaseName = "foo";

    private final Context procedureContext = mock( Context.class );
    private final ResourceTracker resourceTracker = mock( ResourceTracker.class );
    private RoleProcedure roleProcedure;

    public RoleProcedureTest( Function<DatabaseManager<DatabaseContext>,RoleProcedure> roleProcedureFactory )
    {
        this.roleProcedure = roleProcedureFactory.apply( databaseManager );
    }

    @Test
    void shouldThrowWhenDatabaseNameNotSpecified()
    {
        assertThrows( IllegalArgumentException.class, () -> runProcedure( new AnyValue[]{} ) );
    }

    @Test
    void shouldThrowWhenDatabaseNameIsNull()
    {
        assertThrows( IllegalArgumentException.class, () -> runProcedure( new AnyValue[]{null} ) );
    }

    @Test
    void shouldThrowWhenDatabaseNameIsNotString()
    {
        assertThrows( IllegalArgumentException.class, () -> runProcedure( new AnyValue[]{intValue( 42 )} ) );
    }

    @Test
    void shouldThrowIfDatabaseIsNotFound()
    {
        var procedureException = assertThrows( ProcedureException.class, this::runProcedure );
        assertEquals( DatabaseNotFound, procedureException.status() );
        assertThat( procedureException.getMessage(), containsString( "because this database does not exist" ) );
    }

    @Test
    void shouldThrowIfDatabaseIsNotAvailable()
    {
        mockExitingDatabase( false );

        var procedureException = assertThrows( ProcedureException.class, this::runProcedure );

        assertEquals( DatabaseUnavailable, procedureException.status() );
        assertThat( procedureException.getMessage(), containsString( "database is not available" ) );
    }

    @Test
    void shouldHaveCorrectName()
    {
        assertEquals( new QualifiedName( List.of( "dbms", "cluster" ), "role" ), roleProcedure.signature().name() );
    }

    @Test
    void shouldBeASystemProcedure()
    {
        assertTrue( roleProcedure.signature().systemProcedure() );
    }

    RoleInfo runProcedure() throws ProcedureException
    {
        return runProcedure( new AnyValue[]{stringValue( databaseName )} );
    }

    RoleInfo runProcedure( AnyValue[] anyValues ) throws ProcedureException
    {
        var result = roleProcedure.apply( procedureContext, anyValues, resourceTracker );
        assertTrue( result.hasNext() );

        var row = result.next();
        assertFalse( result.hasNext() );
        assertEquals( 1, row.length );

        var element = row[0];
        return RoleInfo.valueOf( ((StringValue) element).stringValue() );
    }

    private void mockExitingDatabase( boolean available )
    {
        var database = mock( Database.class );
        var databaseAvailabilityGuard = mock( DatabaseAvailabilityGuard.class );
        when( databaseAvailabilityGuard.isAvailable() ).thenReturn( available );
        when( databaseContext.database() ).thenReturn( database );
        when( database.getDatabaseAvailabilityGuard() ).thenReturn( databaseAvailabilityGuard );
        when( databaseManager.getDatabaseContext( databaseName ) ).thenReturn( Optional.of( databaseContext ) );
    }

    protected void mockAvailableExitingDatabase()
    {
        mockExitingDatabase( true );
    }
}
