/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.readreplica.CatchupProcessManager;

import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.values.storable.Values.utf8Value;

public class ReadReplicaToggleProcedure extends CallableProcedure.BasicProcedure
{
    private static final List<String> PROCEDURE_NAMESPACE = List.of( "dbms", "cluster" );
    private static final String PROCEDURE_NAME = "readReplicaToggle";
    private static final String DATABASE_PARAMETER = "databaseName";
    private static final String PAUSE_PARAMETER = "pause";
    private final DatabaseManager<?> databaseManager;

    public ReadReplicaToggleProcedure( DatabaseManager<?> databaseManager )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                       .in( DATABASE_PARAMETER, Neo4jTypes.NTString )
                       .in( PAUSE_PARAMETER, Neo4jTypes.NTBoolean )
                       .out( "state", Neo4jTypes.NTString )
                       .description( "The toggle can pause or resume read replica" )
                       .systemProcedure()
                       .admin( true )
                       .build() );
        this.databaseManager = databaseManager;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        final boolean pause = extractPausedFlag( input, PAUSE_PARAMETER, 1 );
        final NamedDatabaseId databaseId = extractDatabaseId( input, DATABASE_PARAMETER, 0 );

        DatabaseContext dbContext = databaseManager
                .getDatabaseContext( databaseId )
                .orElseThrow( () -> new ProcedureException( DatabaseNotFound, "Unable to find database with name " + databaseId.name() ) );
        CatchupProcessManager catchupProcessManager = dbContext.dependencies().resolveDependency( CatchupProcessManager.class );
        String output = executeOperation( catchupProcessManager, pause );

        return RawIterator.<AnyValue[],ProcedureException>of( new AnyValue[]{utf8Value( output )} );
    }

    private boolean extractPausedFlag( AnyValue[] input, String parameterName, int parameterIndex )
    {
        checkParameterIndex( input, parameterIndex );
        checkParameterType( input, parameterName, parameterIndex, BooleanValue.class, "Boolean" );

        return ((BooleanValue) input[parameterIndex]).booleanValue();
    }

    protected NamedDatabaseId extractDatabaseId( AnyValue[] input, String parameterName, int parameterIndex ) throws ProcedureException
    {
        checkParameterIndex( input, parameterIndex );
        checkParameterType( input, parameterName, parameterIndex, TextValue.class, "String" );

        var rawName = (TextValue) input[parameterIndex];

        return databaseManager.databaseIdRepository()
                              .getByName( rawName.stringValue() )
                              .orElseThrow( () -> new ProcedureException( DatabaseNotFound, format( "Unable to retrieve the status " +
                                                                                                    "for database with name %s because no database " +
                                                                                                    "with this name exists!",
                                                                                                    rawName.stringValue() ) ) );
    }

    private void checkParameterIndex( AnyValue[] input, int parameterIndex )
    {
        if ( input.length == 0 )
        {
            throw new IllegalArgumentException( "Illegal input:" + Arrays.toString( input ) );
        }

        if ( parameterIndex < 0 || parameterIndex >= input.length )
        {
            throw new IllegalArgumentException( "Input should contains " + (parameterIndex + 1) + " parameters" );
        }
    }

    private void checkParameterType( AnyValue[] input, String parameterName, int parameterIndex, Class<? extends AnyValue> parameterClass, String expectedType )
    {
        var value = input[parameterIndex];
        if ( !parameterClass.isInstance( value ) )
        {
            throw new IllegalArgumentException(
                    format( "Parameter '%s' value should have a " + expectedType + " representation. Instead: %s", parameterName, value ) );
        }
    }

    private String executeOperation( CatchupProcessManager catchupProcessManager, boolean pause )
    {
        if ( pause )
        {
            try
            {
                if ( catchupProcessManager.pauseCatchupProcess() )
                {
                    return "Catchup process is paused";
                }
                else
                {
                    return "Catchup process was already paused";
                }
            }
            catch ( IllegalStateException ignored )
            {
                return "Catchup process can't be paused";
            }
        }
        else
        {
            if ( catchupProcessManager.resumeCatchupProcess() )
            {
                return "Catchup process is resumed";
            }
            else
            {
                return "Catchup process was already resumed";
            }
        }
    }
}

