/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;

import java.util.Arrays;

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
import org.neo4j.values.storable.TextValue;

import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

abstract class RoleProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "role";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    private static final String OUTPUT_NAME = "role";
    private static final String PARAMETER_NAME = "database";

    protected final DatabaseManager<?> databaseManager;

    RoleProcedure( DatabaseManager<?> databaseManager )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .in( PARAMETER_NAME, Neo4jTypes.NTString )
                .out( OUTPUT_NAME, Neo4jTypes.NTString )
                .description( "The role of this instance in the cluster for the specified database." )
                .systemProcedure()
                .build() );
        this.databaseManager = databaseManager;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        var databaseContext = extractDatabaseContext( input );
        checkAvailable( databaseContext );
        var role = role( databaseContext );
        return RawIterator.<AnyValue[],ProcedureException>of( new AnyValue[]{utf8Value( role.toString() )} );
    }

    private void checkAvailable( DatabaseContext databaseContext ) throws ProcedureException
    {
        if ( !databaseContext.database().getDatabaseAvailabilityGuard().isAvailable() )
        {
            throw new ProcedureException( DatabaseUnavailable,
                                          "Unable to get a cluster role for database '" + databaseContext.database().getNamedDatabaseId() +
                                          " because the database is not available" );
        }
    }

    abstract RoleInfo role( DatabaseContext namedDatabaseId ) throws ProcedureException;

    private DatabaseContext extractDatabaseContext( AnyValue[] input ) throws ProcedureException
    {
        if ( input.length != 1 )
        {
            throw new IllegalArgumentException( "Illegal input: " + Arrays.toString( input ) );
        }
        var value = input[0];
        if ( value instanceof TextValue )
        {
            var databaseName = ((TextValue) value).stringValue();
            return databaseManager.getDatabaseContext( databaseName ).orElseThrow( () -> databaseNotFoundException( databaseName ) );
        }
        throw new IllegalArgumentException( "Parameter '" + PARAMETER_NAME + "' value should be a string: " + value );
    }

    private static ProcedureException databaseNotFoundException( String databaseName )
    {
        return new ProcedureException( DatabaseNotFound,
                "Unable to get a cluster role for database '" + databaseName + "' because this database does not exist" );
    }
}
