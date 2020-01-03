/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;

import java.util.Arrays;

import org.neo4j.collection.RawIterator;
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
        var databaseId = extractDatabaseId( input );
        var role = role( databaseId );
        assertRoleIsKnown( role, databaseId.name() );
        return RawIterator.<AnyValue[],ProcedureException>of( new AnyValue[]{stringValue( role.toString() )} );
    }

    abstract RoleInfo role( NamedDatabaseId namedDatabaseId );

    private NamedDatabaseId extractDatabaseId( AnyValue[] input ) throws ProcedureException
    {
        if ( input.length != 1 )
        {
            throw new IllegalArgumentException( "Illegal input: " + Arrays.toString( input ) );
        }
        var value = input[0];
        if ( value instanceof TextValue )
        {
            var databaseName = ((TextValue) value).stringValue();
            return databaseManager.databaseIdRepository()
                    .getByName( databaseName )
                    .orElseThrow( () -> databaseNotFoundException( databaseName ) );
        }
        throw new IllegalArgumentException( "Parameter '" + PARAMETER_NAME + "' value should be a string: " + value );
    }

    private static void assertRoleIsKnown( RoleInfo role, String databaseName ) throws ProcedureException
    {
        if ( role == RoleInfo.UNKNOWN )
        {
            throw new ProcedureException( DatabaseUnavailable,
                    "Unable to get a cluster role for database '" + databaseName + "' because this database is stopped" );
        }
    }

    private static ProcedureException databaseNotFoundException( String databaseName )
    {
        return new ProcedureException( DatabaseNotFound,
                "Unable to get a cluster role for database '" + databaseName + "' because this database does not exist" );
    }
}
